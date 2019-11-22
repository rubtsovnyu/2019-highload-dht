package ru.mail.polis.service.rubtsov.tank;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Generates config for Yandex.Tank
 */
public final class ConfigGenerator {
    private ConfigGenerator() {
        // Not instantiable
    }

    /**
     * Generates config for Yandex.Tank. The resulting config depends on the parameters.
     * Mode - number of task
     * Address - address of server that will be tested
     * Type - type of load test - test the highest possible rps or test using known
     * Max rps - rps that should be used
     *
     * @param args params for generator
     * @throws IOException if an error occurs during generating config
     */
    public static void main(final String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage:\r\n"
                    + "[mode] - 0..4 - depends on task\r\n"
                    + "[address] - ip:port of server\r\n"
                    + "[type] - 0..1 - test given max rps for 5 minutes (0) "
                    + "or use known (1)\r\n"
                    + "[max rps] - known max rps or rps that should be tested\r\n"
                    + "Config will be saved to the execution directory path");
            System.exit(42);
        }

        final String type = args[2];

        try {
            final int typeInt = Integer.parseInt(type);
            if (typeInt < 0 || typeInt > 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Wrong type!");
        }

        final String mode = args[0];
        final String address = args[1];
        final String maxRps = args[3];

        final ByteArrayOutputStream config = new ByteArrayOutputStream();

        config.write("phantom:\n  address: ".getBytes(UTF_8));
        config.write(address.getBytes(UTF_8));
        config.write("\n  ammofile: /var/loadtest/mode_".getBytes(UTF_8));
        config.write(mode.getBytes(UTF_8));
        config.write(".ammo\n  load_profile:\n    load_type: rps\n    schedule: ".getBytes(UTF_8));
        config.write("line(1, ".getBytes(UTF_8));
        config.write(maxRps.getBytes(UTF_8));
        if (Integer.parseInt(type) == 1) {
            config.write(", 2m)".getBytes(UTF_8));
            config.write(" const(".getBytes(UTF_8));
            config.write(maxRps.getBytes(UTF_8));
            config.write(", 3m)".getBytes(UTF_8));
        } else {
            config.write(", 5m)".getBytes(UTF_8));
        }
        config.write(("\ntelegraf:\n  enabled: false\noverload:\n  enabled: true\n  "
                + "token_file: /var/loadtest/token\n  job_dsc: \"mode: "
                + mode + ", type: " + type + "\"\n").getBytes(UTF_8));

        final String dir = System.getProperty("user.dir");
        final Path filePath = Path.of(dir + "/mode_" + mode + "_type_" + type + ".yaml");
        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }
        final FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile());
        config.writeTo(fileOutputStream);
    }
}
