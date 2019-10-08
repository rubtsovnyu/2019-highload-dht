package ru.mail.polis.dao.rubtsov;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Part of storage located in RAM.
 */

public final class MemTable {
    private final long flushThresholdInBytes;

    private final SortedMap<ByteBuffer, Item> data;
    private long sizeInBytes;

    /**
     * Creates a new RAM-storage.
     *
     * @param heapSizeInBytes given JVM max heap size
     */

    public MemTable(final long heapSizeInBytes) {
        data = new TreeMap<>();
        flushThresholdInBytes = heapSizeInBytes / 16;
    }

    public Iterator<Item> iterator(final ByteBuffer from) {
        return data.tailMap(from).values().iterator();
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */

    public void upsert(final ByteBuffer key, final ByteBuffer value) {
        final Item val = Item.of(key, value);
        calcNewSize(data.put(key, val), val);
    }

    /**
     * Removes the mapping for this key from this table if present.
     *
     * @param key that should be removed
     */

    public void remove(final ByteBuffer key) {
        final Item dead = Item.removed(key);
        calcNewSize(data.put(key, dead), dead);
    }

    private void calcNewSize(final Item previousItem, final Item val) {
        if (previousItem == null) {
            sizeInBytes += val.getSizeInBytes();
        } else {
            sizeInBytes += -previousItem.getSizeInBytes() + val.getSizeInBytes();
        }
    }

    public boolean isFlushNeeded() {
        return sizeInBytes > flushThresholdInBytes;
    }

    /**
     * Drops current MemTable to file.
     *
     * @return path of new SSTable or null if something went wrong during flush
     */

    public Path flush(final File ssTablesDir) throws IOException {
        final Path newSSTablePath = SSTable.writeNewTable(data.values().iterator(), ssTablesDir);
        clear();
        return newSSTablePath;
    }

    public void clear() {
        data.clear();
        sizeInBytes = 0;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }
}
