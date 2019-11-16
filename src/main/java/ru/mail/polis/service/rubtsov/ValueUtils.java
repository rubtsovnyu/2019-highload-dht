package ru.mail.polis.service.rubtsov;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.rubtsov.Item;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

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

    static Value from(@NotNull final HttpResponse<byte[]> response) throws IOException {
        final Optional<String> timestampOptional = response.headers().firstValue(TIMESTAMP_HEADER);
        final String timestamp = timestampOptional.orElse(null);
        if (response.statusCode() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Something wrong with timestamp: " + response.statusCode());
            }
            return Value.present(ByteBuffer.wrap(response.body()), Long.parseLong(timestamp));
        } else if (response.statusCode() == 404) {
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
