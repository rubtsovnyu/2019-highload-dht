package ru.mail.polis.dao.rubtsov;

/**
 * Simple nano time to avoid collisions.
 */

public final class TimeUtils {
    private static long millis;
    private static int additionalTime;

    private TimeUtils() {
    }

    /**
     * Returns current time.
     *
     * @return current time in nanos
     */

    public static long getCurrentTime() {
        final long systemCurrentTime = System.currentTimeMillis();
        if (millis != systemCurrentTime) {
            millis = systemCurrentTime;
            additionalTime = 0;
        }
        return millis * 1_000_000 + additionalTime++;
    }
}
