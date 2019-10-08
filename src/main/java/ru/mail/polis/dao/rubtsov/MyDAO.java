package ru.mail.polis.dao.rubtsov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Simple LSM based {@link DAO} implementation.
 */

public class MyDAO implements DAO {
    private static final int COMPACTION_THRESHOLD = 8;

    private final MemTable memTable;
    private final List<SSTable> ssTables = new ArrayList<>();
    private final File ssTablesDir;
    private final Logger logger = LoggerFactory.getLogger(MyDAO.class);


    /**
     * Constructs a new, empty storage.
     *
     * @param dataFolder      the folder which SSTables will be contained.
     * @param heapSizeInBytes JVM max heap size
     */

    public MyDAO(final File dataFolder, final long heapSizeInBytes) throws IOException {
        memTable = new MemTable(heapSizeInBytes);
        ssTablesDir = dataFolder;
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
    }

    private void initNewSSTable(final File ssTableFile) throws IOException {
        try {
            final SSTable ssTable = new SSTable(ssTableFile);
            ssTables.add(ssTable);
        } catch (IllegalArgumentException e) {
            logger.error("File corrupted: {}, skipped.", ssTableFile.getName());
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Item> itemIterator = itemIterator(from);
        return Iterators.transform(itemIterator, i -> Record.of(i.getKey(), i.getValue()));
    }

    private Iterator<Item> itemIterator(@NotNull final ByteBuffer from) {
        final Collection<Iterator<Item>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable s : ssTables) {
            iterators.add(s.iterator(from));
        }
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIter = Iters.collapseEquals(mergedIter, Item::getKey);
        return Iterators.filter(collapsedIter, i -> !i.isRemoved());
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementExceptionLite {
        final Iterator<Record> iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementExceptionLite("Not found");
        }

        final Record next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementExceptionLite("Not found");
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.isFlushNeeded()) {
            flushTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.isFlushNeeded()) {
            flushTable();
        }
    }

    @Override
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            memTable.flush(ssTablesDir);
        }
    }

    private void flushTable() throws IOException {
        if (ssTables.size() >= COMPACTION_THRESHOLD) {
            compact();
        } else {
            final Path flushedFilePath = memTable.flush(ssTablesDir);
            initNewSSTable(flushedFilePath.toFile());
        }
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Item> itemIterator = itemIterator(Item.TOMBSTONE);
        final Path mergedTable = SSTable.writeNewTable(itemIterator, ssTablesDir);
        ssTables.forEach(s -> removeFile(s.getTableFile().toPath()));
        ssTables.clear();
        memTable.clear();
        initNewSSTable(mergedTable.toFile());
    }

    private void removeFile(final Path p) {
        try {
            Files.delete(p);
        } catch (IOException e) {
            logger.error("Can't remove old file: {}", p.getFileName(), e);
        }
    }
}
