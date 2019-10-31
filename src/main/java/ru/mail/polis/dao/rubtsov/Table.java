package ru.mail.polis.dao.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    /**
     * Get size of table entries in bytes.
     */
    long sizeInBytes();

    /**
     * Get iterator over table entries. Starts from given key.
     */
    Iterator<Item> iterator(@NotNull final ByteBuffer from);

    Iterator<Item> latestIterator(@NotNull final ByteBuffer from);

    /**
     * Put an entry in table.
     */
    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    /**
     * Remove an entry from table.
     */
    void remove(@NotNull ByteBuffer key);

    /**
     * Get the unique identifier associated with the table.
     */
    String getUniqueID();
}
