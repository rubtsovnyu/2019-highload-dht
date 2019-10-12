package ru.mail.polis.dao.rubtsov;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Part of storage located in RAM.
 */

public final class MemTable implements Table {

    private final SortedMap<ByteBuffer, Item> data;
    private final String uniqueID;
    private final AtomicLong sizeInBytes = new AtomicLong();

    /**
     * Creates a new RAM-storage.
     */

    public MemTable() {
        data = new ConcurrentSkipListMap<>();
        uniqueID = UUID.randomUUID().toString();
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

    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    private void calcNewSize(final Item previousItem, final Item val) {
        if (previousItem == null) {
            sizeInBytes.addAndGet(val.getSizeInBytes());
        } else {
            sizeInBytes.addAndGet(-previousItem.getSizeInBytes() + val.getSizeInBytes());
        }
    }

    public String getUniqueID() {
        return uniqueID;
    }

}
