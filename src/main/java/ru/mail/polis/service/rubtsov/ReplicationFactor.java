package ru.mail.polis.service.rubtsov;

import com.google.common.base.Preconditions;

public final class ReplicationFactor {
    private final int ack;
    private final int from;

    private ReplicationFactor(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    /**
     * Create replication factor from string in format "[ack]/[from]".
     *
     * @param replicas String with r/f
     * @return Replication factor
     */
    public static ReplicationFactor from(final String replicas) {
        final int separatorIndex = replicas.indexOf('/');
        Preconditions.checkArgument(separatorIndex != -1, "Invalid replicas!");
        final int ack = Integer.parseInt(replicas.substring(0, separatorIndex));
        final int from = Integer.parseInt(replicas.substring(separatorIndex + 1));
        return new ReplicationFactor(ack, from);
    }

    static ReplicationFactor quorum(final int nodesCount) {
        final int quorum = nodesCount / 2 + 1;
        return new ReplicationFactor(quorum, nodesCount);
    }

    int getAck() {
        return ack;
    }

    int getFrom() {
        return from;
    }
}
