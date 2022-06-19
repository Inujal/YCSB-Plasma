package base;

import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import util.InetSocketAddressConverter;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public abstract class KeyValueStore extends DB {

    public static final String ADDRESS_KEY = "org.jb.plasma.benchmark.server";
    private static final String NAMESPACE_SEPARATOR = ".";

    private static String generateKey(final String table, final String key) {
        return table.concat(NAMESPACE_SEPARATOR).concat(key);
    }

    @Override
    public void init() throws DBException {
        final var address = getProperties().getProperty(ADDRESS_KEY);
        if (address == null) {
            throw new RuntimeException("The server address is not set. Please set it using the " + ADDRESS_KEY + " property.");
        }

        try {
            initialize(InetSocketAddressConverter.from(address));
        } catch (final NetworkException e) {
            throw new DBException("Initializing client connection failed.", e);
        }
    }

    /**
     * Initializes this binding instance and connects to the remote server.
     *
     * @param serverAddress The server instance's address.
     * @throws NetworkException If the network connection fails.
     */
    public abstract void initialize(InetSocketAddress serverAddress) throws NetworkException;

    /**
     * Retrieves an object.
     *
     * @param key The key, under which the object is stored.
     * @return The object.
     * @throws NetworkException     If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    public abstract byte[] get(String key) throws NetworkException, KeyNotFoundException;

    /**
     * Stores an object.
     *
     * @param key   The key, under which the object shall be stored.
     * @param value The object to store.
     * @return A YCSB {@link Status} code.
     * @throws NetworkException If the network connection fails.
     */
    public abstract Status put(String key, byte[] value) throws NetworkException;

    /**
     * Deletes the given key.
     *
     * @param key The key to be deleted.
     * @return A YCSB {@link Status} code.
     * @throws NetworkException     If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    public abstract Status delete(String key) throws NetworkException, KeyNotFoundException;

    @Override
    public Status read(final String table, final String key, final Set<String> fields, final Map<String, ByteIterator> result) {
        if (fields != null && fields.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }

        try {
            final var value = get(generateKey(table, key));

            if (fields == null) {
                result.put("field0", new ByteArrayByteIterator(value));
            } else {
                result.put(fields.iterator().next(), new ByteArrayByteIterator(value));
            }

            return Status.OK;
        } catch (final NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        } catch (final KeyNotFoundException e) {
            return Status.NOT_FOUND;
        }
    }

    @Override
    public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                       final Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
        if (values.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }
        final String _key = generateKey(table, key);
        try {
            delete(_key);
        } catch (final NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        } catch (final KeyNotFoundException e) {
            return Status.NOT_FOUND;
        }
        return insert(table, key, values);
    }

    @Override
    public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
        if (values.size() != 1) {
            System.err.println("Field counts other than 1 are not supported!");
            return Status.BAD_REQUEST;
        }

        try {
            return put(generateKey(table, key), values.values().iterator().next().toArray());
        } catch (final NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    @Override
    public Status delete(final String table, final String key) {
        try {
            return delete(generateKey(table, key));
        } catch (final NetworkException e) {
            return Status.SERVICE_UNAVAILABLE;
        } catch (final KeyNotFoundException e) {
            return Status.NOT_FOUND;
        }
    }
}

