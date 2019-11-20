package ru.mail.polis.service.rubtsov.tank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class AmmoGenerator {
    private static final int VALUE_LENGTH = 256;

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: [mode] [requests count] (optional)[last key]");
            System.exit(42);
        }

        final int mode = Integer.parseInt(args[0]);
        final long requestsCount = Long.parseLong(args[1]);

        long lastKey = 0;

        if (args.length == 3) {
            lastKey = Long.parseLong(args[2]);
        }

        switch (mode) {
            case 0:
                uniquePUTs(requestsCount);
                break;
            case 1:
                putWith10PartOverwrite(requestsCount);
                break;
            case 2:
                getExistingKeysUniformly(requestsCount);
                break;
            case 3:
                getExistingKeysPreferNew(requestsCount);
                break;
            case 4:
                mixedPutNewGetExisting(requestsCount, lastKey);
                break;
            default:
                System.err.println("Invalid mode!");
                System.exit(42);
        }
    }

    private static void uniquePUTs(final long count) throws IOException {
        for (long i = 0; i < count; i++) {
            put(i);
        }
    }

    private static void putWith10PartOverwrite(final long count) throws IOException {
        long nextKeyToRecord = 0;
        for (long i = 0; i < count; i++) {
            final boolean isOverwrite = ThreadLocalRandom.current().nextInt(10) == 0;
            if (isOverwrite) {
                final long keyToOverwrite = ThreadLocalRandom.current().nextLong(nextKeyToRecord);
                put(keyToOverwrite);
            } else {
                put(nextKeyToRecord);
                nextKeyToRecord++;
            }
        }
    }

    private static void getExistingKeysUniformly(final long count) throws IOException {
        for (long i = 0; i < count; i++) {
            final long key = ThreadLocalRandom.current().nextLong(count);
            get(key);
        }
    }

    private static void getExistingKeysPreferNew(final long count) throws IOException {
        for (long i = 0; i < count; i++) {
            final long key = getRightValByGaussian(count);
            get(key);
        }
    }

    private static void mixedPutNewGetExisting(final long count, final long lastKey) throws IOException {
        long newPutKey = lastKey + 1;
        for (long i = lastKey; i < count + lastKey; i++) {
            final boolean putOrGet = ThreadLocalRandom.current().nextBoolean();
            if (putOrGet) {
                put(newPutKey);
                newPutKey++;
            } else {
                final long key = ThreadLocalRandom.current().nextLong(newPutKey);
                get(key);
            }
        }
    }

    private static void put(final long keyLong) throws IOException {
        final String key = keyFromLong(keyLong);
        final byte[] value = randomValue();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + "\r\n");
            writer.write("\r\n");
        }
        request.write(value);
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" PUT\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    private static void get(final long keyLong) throws IOException {
        final String key = keyFromLong(keyLong);
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("\r\n");
        }
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" GET\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    private static String keyFromLong(final long key) {
        return String.valueOf(key);
    }

    private static byte[] randomValue() {
        final byte[] value = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    //Get value for mean = val and st.dev = -abs(val)
    private static long getRightValByGaussian(final long val) {
        boolean isDone = false;
        long fin = 0;
        while (!isDone) {
            final double gaussian = ThreadLocalRandom.current().nextGaussian();
            final double gDivided = gaussian / 3;
            if (gDivided < -1 || gDivided > 0) continue;
            final double gMmax = gDivided * (val + 1);
            final long rounded = Math.round(gMmax);
            final long abs = Math.abs(rounded);
            fin = -abs + val + 1;
            if (fin > val) continue;
            isDone = true;
        }
        return fin;
    }
}
