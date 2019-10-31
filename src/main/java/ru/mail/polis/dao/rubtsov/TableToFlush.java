package ru.mail.polis.dao.rubtsov;

class TableToFlush {
    private final Table table;
    private final boolean poisonPill;

    TableToFlush(final Table table) {
        this(table, false);
    }

    TableToFlush(final Table table, final boolean poisonPill) {
        this.table = table;
        this.poisonPill = poisonPill;
    }

    Table getTable() {
        return table;
    }

    boolean isPoisonPill() {
        return poisonPill;
    }
}
