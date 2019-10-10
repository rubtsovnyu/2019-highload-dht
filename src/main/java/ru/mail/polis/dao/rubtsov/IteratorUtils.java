package ru.mail.polis.dao.rubtsov;

import com.google.common.collect.Iterators;
import ru.mail.polis.dao.Iters;

import java.util.Collection;
import java.util.Iterator;

public class IteratorUtils {
    public static Iterator<Item> itersTransform(Collection<Iterator<Item>> iterators) {
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIter = Iters.collapseEquals(mergedIter, Item::getKey);
        return Iterators.filter(collapsedIter, i -> !i.isRemoved());
    }
}
