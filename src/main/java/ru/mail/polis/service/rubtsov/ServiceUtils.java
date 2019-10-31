package ru.mail.polis.service.rubtsov;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import static ru.mail.polis.service.rubtsov.MyService.TIMESTAMP_HEADER;

public class ServiceUtils {
    private ServiceUtils() {
    }

    static Response from(@NotNull final Value value, final boolean proxy) {
        Response response;
        switch (value.getState()) {
            case ABSENT:
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
                return response;
            case PRESENT:
                response = new Response(Response.OK, value.getData().array());
                if (proxy) {
                    response.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return response;
            case REMOVED:
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxy) {
                    response.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return response;
            default:
                throw new IllegalArgumentException("Invalid data");
        }
    }
}
