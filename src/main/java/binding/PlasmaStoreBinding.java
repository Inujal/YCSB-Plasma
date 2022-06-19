package binding;

import base.KeyValueStore;
import exceptions.KeyNotFoundException;
import exceptions.NetworkException;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import site.ycsb.Status;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class PlasmaStoreBinding extends KeyValueStore {

    /**
     * The client instance used for all operations.
     */
    private PlasmaClient client;
    private ArrayList<byte[]> storedIDs = new ArrayList<>();

    /**
     * Initializes this binding instance and connects to the remote server.
     *
     * @param serverAddress The server instance's address.
     */
    @Override
    public void initialize(final InetSocketAddress serverAddress) throws NetworkException {
        try {
            System.loadLibrary("plasma_java");
            client = new PlasmaClient("/tmp/plasma", "", 0);
        } catch (final Exception e) {
            throw new NetworkException(e.getMessage());
        }
    }

    /**
     * Retrieves an object.
     *
     * @param key The key, under which the object is stored.
     * @return The object.
     * @throws NetworkException     If the network connection fails.
     * @throws KeyNotFoundException If the specified key does not exist.
     */
    @Override
    public byte[] get(final String key) throws NetworkException, KeyNotFoundException {
        ByteBuffer buffer;
        try {
            final byte[] id = generateID(key);
            buffer = client.getObjAsByteBuffer(id, 500, false);
        } catch (final Exception e) {
            throw new NetworkException(e.getMessage());
        }
        if (ObjectUtils.isEmpty(buffer)) {
            throw new KeyNotFoundException("not found");
        }
        final byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return data;
    }

    /**
     * Stores an object.
     *
     * @param key   The key, under which the object shall be stored.
     * @param value The object to store.
     * @return A YCSB {@link Status} code.
     */
    @Override
    public Status put(final String key, final byte[] value) {
        try {
            final byte[] id = generateID(key);
            final ByteBuffer byteBuffer = client.create(id, value.length, new byte[0]);
            for (final byte b : value) {
                byteBuffer.put(b);
            }
            client.seal(id);
            storedIDs.add(id);
        } catch (final DuplicateObjectException e) {
            return Status.BAD_REQUEST;
        } catch (final Exception e) {
            return Status.SERVICE_UNAVAILABLE;
        }
        return Status.OK;
    }

    /**
     * Deletes the given key.
     *
     * @param key The key to be deleted.
     * @return A YCSB {@link Status} code.
     */
    @Override
    public Status delete(final String key) {
        try {
            final byte[] id = generateID(key);
            client.delete(id);
            while (client.contains(id)) {
                if (!storedIDs.contains(id)) {
                    return Status.BAD_REQUEST;
                }
                client.release(id);
                client.delete(id);
            }
            storedIDs.remove(id);
        } catch (final Exception e) {
            return Status.SERVICE_UNAVAILABLE;
        }
        return Status.OK;
    }

    public byte[] generateID(final String key) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final byte[] id = messageDigest.digest(key.getBytes(StandardCharsets.UTF_8));
        return ArrayUtils.addAll(id, new byte[4]);
    }
}

