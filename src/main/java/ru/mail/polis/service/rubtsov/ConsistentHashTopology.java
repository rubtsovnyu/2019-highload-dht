package ru.mail.polis.service.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ConsistentHashTopology implements Topology<String> {
    private final int range;
    @NotNull
    private final Map<Integer, String> nodes;
    @NotNull
    private final String me;

    /**
     * Create a consistent hashing topology.
     *
     * @param range Range for CH
     * @param nodes All nodes
     * @param me    Current node
     */
    public ConsistentHashTopology(final int range,
                                  @NotNull final Set<String> nodes,
                                  @NotNull final String me) {
        this.range = range;
        this.nodes = new HashMap<>(range * 2 + 1);
        this.me = me;
        int offset = 0;
        for (final String node :
                nodes) {
            for (int i = -range + offset; i <= range; i += nodes.size()) {
                this.nodes.put(i, node);
            }
            offset++;
        }
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        final int keyHashCode = key.hashCode();
        return nodes.get((keyHashCode & Integer.MAX_VALUE) % range);
    }

    @NotNull
    @Override
    public Set<String> all() {
        return new TreeSet<>(nodes.values());
    }
}
