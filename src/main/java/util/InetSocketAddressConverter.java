package util;

import picocli.CommandLine;

import java.net.InetSocketAddress;

public class InetSocketAddressConverter implements CommandLine.ITypeConverter<InetSocketAddress> {

    private final int defaultPort;

    public InetSocketAddressConverter(final int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public InetSocketAddress convert(final String address) {
        final var splittedAddress = address.split(":");
        if (splittedAddress.length == 0 || splittedAddress.length > 2) {
            throw new CommandLine.TypeConversionException("No connection string specified");
        }

        final var hostname = splittedAddress[0];
        var port = defaultPort;
        if (splittedAddress.length > 1) {
            try {
                port = Integer.parseInt(splittedAddress[1]);
            } catch (final NumberFormatException e) {
                throw new CommandLine.TypeConversionException("Invalid port specified");
            }
        }

        try {
            return new InetSocketAddress(hostname, port);
        } catch (final IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(e.getMessage());
        }
    }

    private static final InetSocketAddressConverter INSTANCE = new InetSocketAddressConverter(2998);

    public static InetSocketAddress from(final String address) {
        try {
            return INSTANCE.convert(address);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Converting address failed.", e);
        }
    }
}
