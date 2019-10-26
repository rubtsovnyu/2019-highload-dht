package ru.mail.polis.service.rubtsov;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class BasicTopology implements Topology<String> {
    @NotNull
    private final String me;
    @NotNull
    private final String[] nodes;

    public BasicTopology(@NotNull String me, @NotNull Set<String> nodes) {
        this.me = me;
        Preconditions.checkArgument(nodes.contains(me), "Invalid topology");
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public boolean isMe(@NotNull String node) {
        return me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull ByteBuffer key) {
        final int keyHashCode = key.hashCode();
        final int node = (keyHashCode & Integer.MAX_VALUE) % nodes.length;
        return nodes[node];
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }
}
