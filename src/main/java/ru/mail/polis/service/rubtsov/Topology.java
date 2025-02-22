package ru.mail.polis.service.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public interface Topology<T> {
    boolean isMe(@NotNull T node);

    @NotNull
    T primaryFor(@NotNull ByteBuffer key);

    @NotNull
    Set<T> all();

    int size();

    @NotNull
    List<T> replicas(final int ack, @NotNull final ByteBuffer key);

    @NotNull
    String me();
}
