package ru.mail.polis.dao.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTablePool implements Table, Closeable {
    private final NavigableMap<String, Table> pendingFlush;
    private final BlockingQueue<TableToFlush> flushQueue;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final long flushThresholdInBytes;
    private final AtomicBoolean stopFlag = new AtomicBoolean();
    private MemTable currentMemTable;

    MemTablePool(final long flushThresholdInBytes) {
        this.flushThresholdInBytes = flushThresholdInBytes;
        currentMemTable = new MemTable();
        pendingFlush = new ConcurrentSkipListMap<>();
        flushQueue = new ArrayBlockingQueue<>(8);
    }

    @Override
    public long sizeInBytes() {
        readWriteLock.readLock().lock();
        try {
            long sizeInBytes = currentMemTable.sizeInBytes();
            for (final Table table :
                    pendingFlush.values()) {
                sizeInBytes += table.sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public int size() {
        return pendingFlush.size() + 1;
    }

    @Override
    public Iterator<Item> iterator(@NotNull final ByteBuffer from) {
        readWriteLock.readLock().lock();
        final Collection<Iterator<Item>> iterators;
        try {
            iterators = new ArrayList<>(pendingFlush.size() + 1);
            iterators.add(currentMemTable.iterator(from));
            for (final Table table : pendingFlush.values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        return IteratorUtils.itersTransform(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        if (stopFlag.get()) {
            throw new IllegalStateException("Stopped");
        }
        readWriteLock.readLock().lock();
        try {
            currentMemTable.upsert(key, value);
        } finally {
            readWriteLock.readLock().unlock();
        }
        enqueueFlush();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (stopFlag.get()) {
            throw new IllegalStateException("Stopped");
        }
        readWriteLock.readLock().lock();
        try {
            currentMemTable.remove(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
        enqueueFlush();
    }

    @Override
    public String getUniqueID() {
        return currentMemTable.getUniqueID();
    }

    private void enqueueFlush() {
        if (currentMemTable.sizeInBytes() > flushThresholdInBytes) {
            readWriteLock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (currentMemTable.sizeInBytes() > flushThresholdInBytes) {
                    tableToFlush = new TableToFlush(currentMemTable);
                    pendingFlush.put(currentMemTable.getUniqueID(), currentMemTable);
                    currentMemTable = new MemTable();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
            if (tableToFlush != null) {
                try {
                    flushQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void clear() {
        currentMemTable = new MemTable();
        pendingFlush.clear();
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    void flushed(final String uniqueID) {
        readWriteLock.writeLock().lock();
        try {
            pendingFlush.remove(uniqueID);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!stopFlag.compareAndSet(false, true)) {
            return;
        }
        readWriteLock.writeLock().lock();
        TableToFlush tableToFlush;
        try {
            tableToFlush = new TableToFlush(currentMemTable, true);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
