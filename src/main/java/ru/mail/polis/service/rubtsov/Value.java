package ru.mail.polis.service.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {
    private static final Value ABSENT = new Value(null, -1, State.ABSENT);

    private final ByteBuffer data;
    private final long timestamp;
    private final State state;

    private Value(final ByteBuffer data, final long timestamp, final State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    static Value present(final ByteBuffer data, final long timestamp) {
        return new Value(data.duplicate(), timestamp, State.PRESENT);
    }

    static Value absent() {
        return ABSENT;
    }

    static Value removed(final long timestamp) {
        return new Value(null, timestamp, State.REMOVED);
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return Long.compare(o.timestamp, this.timestamp);
    }

    State getState() {
        return state;
    }

    public ByteBuffer getData() {
        return data;
    }

    long getTimestamp() {
        return timestamp;
    }

    public enum State {
        ABSENT,
        PRESENT,
        REMOVED
    }
}
