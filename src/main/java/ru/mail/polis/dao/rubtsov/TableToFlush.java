package ru.mail.polis.dao.rubtsov;

public class TableToFlush {
    private final Table table;
    private final boolean poisonPill;

    public TableToFlush(final Table table) {
        this(table, false);
    }

    public TableToFlush(final Table table, final boolean poisonPill) {
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public Table getTable() {
        return table;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }
}
