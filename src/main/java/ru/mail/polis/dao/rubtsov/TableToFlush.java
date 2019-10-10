package ru.mail.polis.dao.rubtsov;

public class TableToFlush {
    private final Table table;
    private final boolean poisonPill;

    public TableToFlush(Table table) {
        this(table, false);
    }

    public TableToFlush(Table table, boolean poisonPill) {
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
