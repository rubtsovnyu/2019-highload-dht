package ru.mail.polis.dao.rubtsov;

import java.util.NoSuchElementException;

public class NoSuchElementExceptionLite extends NoSuchElementException {
    public NoSuchElementExceptionLite(String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
