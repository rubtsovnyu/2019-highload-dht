package ru.mail.polis.service.rubtsov;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.List;

import static ru.mail.polis.service.rubtsov.MyService.TIMESTAMP_HEADER;

final class ServiceUtils {
    private static final Logger logger = LoggerFactory.getLogger(ServiceUtils.class);
    private static final String RESPONSE_ERROR_MSG = "Can't send a response";

    private ServiceUtils() {
    }

    static Response from(@NotNull final Value value, final boolean proxy) {
        Response response;
        switch (value.getState()) {
            case ABSENT:
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
                return response;
            case PRESENT:
                response = new Response(Response.OK, value.getDataBytes());
                if (proxy) {
                    response.addHeader(TIMESTAMP_HEADER + ": " + value.getTimestamp());
                }
                return response;
            case REMOVED:
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxy) {
                    response.addHeader(TIMESTAMP_HEADER + ": " + value.getTimestamp());
                }
                return response;
            default:
                throw new IllegalArgumentException("Invalid data");
        }
    }

    static void handleGetResponses(final boolean haveOneAlready,
                                   final int ack,
                                   @NotNull final List<Value> values,
                                   @NotNull final List<HttpResponse<byte[]>> httpResponses,
                                   @NotNull final HttpSession session) {
        int resAck = haveOneAlready ? 1 : 0;
        for (final HttpResponse<byte[]> r :
                httpResponses) {
            try {
                values.add(ValueUtils.from(r));
                resAck++;
            } catch (IOException e) {
                logger.error("Can't add value to DAO", e);
            }
        }
        try {
            if (resAck >= ack) {
                session.sendResponse(ServiceUtils.from(ValueUtils.merge(values), false));
            } else {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            }
        } catch (IOException e) {
            logger.error(RESPONSE_ERROR_MSG, e);
        }
    }

    static void handlePutOrDeleteResponses(final boolean haveOneAlready,
                                           final int ack,
                                           @NotNull final List<HttpResponse<byte[]>> httpResponses,
                                           @NotNull final HttpSession session,
                                           final int neededStatusCode) {
        int resAck = haveOneAlready ? 1 : 0;
        for (final HttpResponse<byte[]> r :
                httpResponses) {
            if (r.statusCode() == neededStatusCode) {
                resAck++;
            }
        }
        try {
            if (resAck >= ack) {
                if (neededStatusCode == 201) {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                } else if (neededStatusCode == 202) {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                }
            } else {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            }
        } catch (IOException e) {
            logger.error(RESPONSE_ERROR_MSG, e);
        }
    }
}
