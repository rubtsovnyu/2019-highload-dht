package ru.mail.polis.service.rubtsov;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.rubtsov.Item;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import static ru.mail.polis.service.rubtsov.MyService.TIMESTAMP_HEADER;

final class ValueUtils {
    private ValueUtils() {
    }

    static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(value -> value.getState() != Value.State.ABSENT)
                .max(Comparator.comparingLong(Value::getTimestamp))
                .orElseGet(Value::absent);
    }

    static Value from(@NotNull final Response response) throws IOException {
        final String timestamp = response.getHeader(TIMESTAMP_HEADER);
        if (response.getStatus() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Something wrong with timestamp: " + response.getStatus());
            }
            return Value.present(ByteBuffer.wrap(response.getBody()), Long.parseLong(timestamp));
        } else if (response.getStatus() == 404) {
            if (timestamp == null) {
                return Value.absent();
            } else {
                return Value.removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IOException();
        }
    }

    static Value from(@NotNull final ByteBuffer key,
                      @NotNull final Iterator<Item> items) {
        if (!items.hasNext()) {
            return Value.absent();
        }

        final Item item = items.next();

        if (!item.getKey().equals(key)) {
            return Value.absent();
        }

        if (item.isRemoved()) {
            return Value.removed(item.getTimeStampAbs());
        } else {
            return Value.present(item.getValue(), item.getTimeStamp());
        }
    }
}
