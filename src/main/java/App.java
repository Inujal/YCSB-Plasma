import runner.BenchmarkRunner;
import util.InetSocketAddressConverter;
import picocli.CommandLine;

import java.net.InetSocketAddress;

public class App {

    public static void main(final String[] args) {
        final var exitCode = new CommandLine(new BenchmarkRunner())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(2998))
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);

        System.exit(exitCode);
    }
}
