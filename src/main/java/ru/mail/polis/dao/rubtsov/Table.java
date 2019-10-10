package ru.mail.polis.dao.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    long sizeInBytes();

    Iterator<Item> iterator(@NotNull final ByteBuffer from);

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    String getUniqueID();
}
