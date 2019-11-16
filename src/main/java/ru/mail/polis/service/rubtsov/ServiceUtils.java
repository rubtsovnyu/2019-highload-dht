package ru.mail.polis.service.rubtsov;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.mail.polis.service.rubtsov.MyService.ENTITY_PATH;
import static ru.mail.polis.service.rubtsov.MyService.PROXY_HEADER;
import static ru.mail.polis.service.rubtsov.MyService.TIMEOUT;
import static ru.mail.polis.service.rubtsov.MyService.TIMESTAMP_HEADER;

final class ServiceUtils {
    private static final Logger logger = LoggerFactory.getLogger(ServiceUtils.class);

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

    @NotNull
    static List<HttpRequest> getHttpRequests(Topology<String> topology,
                                             final String id,
                                             final Function<HttpRequest.Builder, HttpRequest.Builder> method) {
        return topology.all().stream()
                .filter(n -> !topology.isMe(n))
                .map(s -> s + ENTITY_PATH + id)
                .map(URI::create)
                .map(HttpRequest::newBuilder)
                .map(method)
                .map(b -> b.header(PROXY_HEADER, Boolean.TRUE.toString())
                        .timeout(Duration.ofMillis(TIMEOUT))
                        .build())
                .collect(Collectors.toList());
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
            logger.error("Can't send a response", e);
        }
    }

    static void handlePutResponses(final boolean haveOneAlready,
                                   final int ack,
                                   @NotNull final List<HttpResponse<byte[]>> httpResponses,
                                   @NotNull final HttpSession session) {
        int resAck = haveOneAlready ? 1 : 0;
        for (final HttpResponse<byte[]> r :
                httpResponses) {
            if (r.statusCode() == 201) {
                resAck++;
            }
        }
        try {
            if (resAck >= ack) {
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            } else {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            }
        } catch (IOException e) {
            logger.error("Can't send a response", e);
        }
    }

    static void handleDeleteResponses(final boolean haveOneAlready,
                                      final int ack,
                                      @NotNull final List<HttpResponse<byte[]>> httpResponses,
                                      @NotNull final HttpSession session) {
        int resAck = haveOneAlready ? 1 : 0;
        for (final HttpResponse<byte[]> r :
                httpResponses) {
            if (r.statusCode() == 202) {
                resAck++;
            }
        }
        try {
            if (resAck >= ack) {
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            } else {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            }
        } catch (IOException e) {
            logger.error("Can't send a response", e);
        }
    }
}
