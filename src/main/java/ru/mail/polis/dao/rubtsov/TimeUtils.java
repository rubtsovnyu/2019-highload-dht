package ru.mail.polis.dao.rubtsov;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple nano time to avoid collisions.
 */

public final class TimeUtils {
    private static AtomicLong millis = new AtomicLong();
    private static AtomicInteger additionalTime = new AtomicInteger();

    private TimeUtils() {
    }

    /**
     * Returns current time.
     *
     * @return current time in nanos
     */

    public static long getCurrentTime() {
        final long systemCurrentTime = System.currentTimeMillis();
        if (millis.getAndSet(systemCurrentTime) != systemCurrentTime) {
            additionalTime.set(0);
        }
        return millis.get() * 1_000_000 + additionalTime.getAndIncrement();
    }
}
