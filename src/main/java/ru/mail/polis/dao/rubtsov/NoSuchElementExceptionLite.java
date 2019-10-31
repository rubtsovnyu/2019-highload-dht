package ru.mail.polis.dao.rubtsov;

import java.util.NoSuchElementException;

public class NoSuchElementExceptionLite extends NoSuchElementException {
    private static final long serialVersionUID = 6769829250639411879L;

    public NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
