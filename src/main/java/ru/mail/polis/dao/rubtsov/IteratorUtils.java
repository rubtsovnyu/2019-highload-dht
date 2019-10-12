package ru.mail.polis.dao.rubtsov;

import com.google.common.collect.Iterators;
import ru.mail.polis.dao.Iters;

import java.util.Collection;
import java.util.Iterator;

public final class IteratorUtils {
    private IteratorUtils() {
    }

    /**
     * Receives some {@link Collection} of iterators over {@link Item}s, merges it,
     * collapses equals by key and filters them according to existence
     * @param iterators collection with iterators
     * @return iterator after all transformations
     */
    public static Iterator<Item> itersTransform(final Collection<Iterator<Item>> iterators) {
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIter = Iters.collapseEquals(mergedIter, Item::getKey);
        return Iterators.filter(collapsedIter, i -> !i.isRemoved());
    }
}
