package ru.mail.polis.dao.rubtsov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Simple LSM based {@link DAO} implementation.
 */
public class MyDAO implements DAO {
    private final MemTablePool memTablePool;
    private final List<SSTable> ssTables;
    private final File ssTablesDir;
    private final Logger logger = LoggerFactory.getLogger(MyDAO.class);
    private final FlushThread flushThread;
    private final ReadWriteLock readWriteLock;

    /**
     * Constructs a new, empty storage.
     *
     * @param dataFolder      the folder which SSTables will be contained.
     * @param heapSizeInBytes JVM max heap size
     */
    public MyDAO(final File dataFolder, final long heapSizeInBytes) throws IOException {
        memTablePool = new MemTablePool(heapSizeInBytes / 64);
        ssTablesDir = dataFolder;
        ssTables = new CopyOnWriteArrayList<>();
        try (Stream<Path> files = Files.list(ssTablesDir.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(SSTable.VALID_FILE_EXTENSTION))
                    .forEach(p -> {
                        try {
                            initNewSSTable(p.toFile());
                        } catch (IOException e) {
                            logger.error("Init of SSTable failed: {}", p.getFileName(), e);
                        }
                    });
        }
        flushThread = new FlushThread();
        flushThread.start();
        readWriteLock = new ReentrantReadWriteLock();
        logger.info("DAO in {} created", ssTablesDir.getAbsolutePath());
    }

    private void initNewSSTable(final File ssTableFile) throws IOException {
        try {
            final SSTable ssTable = new SSTable(ssTableFile);
            ssTables.add(ssTable);
        } catch (IllegalArgumentException e) {
            logger.error("File corrupted: {}, skipped.", ssTableFile.getName(), e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Item> itemIterator = itemIterator(from);
        return Iterators.transform(itemIterator, i -> Record.of(i.getKey(), i.getValue()));
    }

    private Iterator<Item> itemIterator(@NotNull final ByteBuffer from) {
        return IteratorUtils.itersTransformWithoutRemoved(collectItems(from));
    }

    @NotNull
    @Override
    public Iterator<Item> latestIterator(@NotNull final ByteBuffer from) {
        return IteratorUtils.itersTransformWithRemoved(collectItems(from));
    }

    private Collection<Iterator<Item>> collectItems(@NotNull final ByteBuffer from) {
        final Collection<Iterator<Item>> iterators;
        readWriteLock.readLock().lock();
        try {
            iterators = new ArrayList<>(ssTables.size() + memTablePool.size());
            iterators.add(memTablePool.latestIterator(from));
            for (final Table s : ssTables) {
                iterators.add(s.iterator(from));
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        return iterators;
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementExceptionLite {
        final var iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementExceptionLite("Not found");
        }

        final Record next = iter.next();
        if (!next.getKey().equals(key)) {
            throw new NoSuchElementExceptionLite("Not found");
        }
        return next.getValue();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTablePool.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTablePool.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTablePool.close();
        try {
            flushThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("DAO in {} closed", ssTablesDir.getAbsolutePath());
    }

    private void flushTable(final Table table) throws IOException {
        final Iterator<Item> iterator = table.latestIterator(ByteBuffer.allocate(0));
        final Path flushedFilePath = SSTable.writeNewTable(iterator, ssTablesDir, table.getUniqueID());
        initNewSSTable(flushedFilePath.toFile());
    }

    @Override
    public void compact() throws IOException {
        final Collection<Iterator<Item>> iterators;
        readWriteLock.readLock().lock();
        try {
            iterators = new ArrayList<>();
            for (final Table s : ssTables) {
                iterators.add(s.iterator(ByteBuffer.allocate(0)));
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        final Iterator<Item> itersTransform = IteratorUtils.itersTransformWithoutRemoved(iterators);
        final Path compactioned = SSTable.writeNewTable(itersTransform, ssTablesDir, UUID.randomUUID().toString());
        readWriteLock.writeLock().lock();
        try {
            ssTables.forEach(s -> removeFile(s.getTableFile().toPath()));
            ssTables.clear();
            initNewSSTable(compactioned.toFile());
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void removeFile(final Path p) {
        try {
            Files.delete(p);
        } catch (IOException e) {
            logger.error("Can't remove old file: {}", p.getFileName(), e);
        }
    }

    private class FlushThread extends Thread {
        FlushThread() {
            super("flusher");
        }

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!poisonReceived && !isInterrupted()) {
                logger.info("Number of files: {}", ssTables.size());
                TableToFlush tableToFlush = null;
                try {
                    tableToFlush = memTablePool.takeToFlush();
                    poisonReceived = tableToFlush.isPoisonPill();
                    if (tableToFlush.getTable().sizeInBytes() > 0) {
                        flushTable(tableToFlush.getTable());
                    }
                    memTablePool.flushed(tableToFlush.getTable().getUniqueID());
                } catch (InterruptedException e) {
                    interrupt();
                } catch (IOException e) {
                    logger.error("Error while flushing a table {}", tableToFlush.getTable().getUniqueID(), e);
                }
            }
            if (poisonReceived) {
                logger.info("Poison pill received. Flushing stopped.");
            }
        }
    }
}
