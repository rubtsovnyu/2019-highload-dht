package ru.mail.polis.dao.rubtsov;

import com.google.common.collect.Iterators;
import ru.mail.polis.dao.Iters;

import java.util.Collection;
import java.util.Iterator;

final class IteratorUtils {
    private IteratorUtils() {
    }

    /**
     * Receives some {@link Collection} of iterators over {@link Item}s, merges it,
     * collapses equals by key and filters them according to existence.
     * @param iterators collection with iterators
     * @return iterator after all transformations
     */
    static Iterator<Item> itersTransformWithoutRemoved(final Collection<Iterator<Item>> iterators) {
        return Iterators.filter(itersTransformWithRemoved(iterators), i -> !i.isRemoved());
    }

    static Iterator<Item> itersTransformWithRemoved(final Collection<Iterator<Item>> iterators) {
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        return Iters.collapseEquals(mergedIter, Item::getKey);
    }
}
