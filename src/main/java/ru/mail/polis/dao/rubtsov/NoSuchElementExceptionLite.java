package ru.mail.polis.dao.rubtsov;

import java.util.NoSuchElementException;

public class NoSuchElementExceptionLite extends NoSuchElementException {
    public NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
