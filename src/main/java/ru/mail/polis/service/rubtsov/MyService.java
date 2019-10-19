package ru.mail.polis.service.rubtsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;

public class MyService extends HttpServer implements Service {
    private final DAO dao;

    public MyService(final int port,
                     @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port) {
        final HttpServerConfig serverConfig = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }

    /**
     * Receives a request to an entity and respond depending on the method.
     * @param id Entity iD
     * @param request HTTP request
     * @return HTTP response
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id,
                           final Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(id);
                case Request.METHOD_PUT:
                    return upsert(id, request.getBody());
                case Request.METHOD_DELETE:
                    return remove(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (Exception e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response get(final String key) throws IOException {
        final ByteBuffer value;
        try {
            value = dao.get(ByteBuffer.wrap(key.getBytes(UTF_8))).duplicate();
            final byte[] responseBody = new byte[value.remaining()];
            value.get(responseBody);
            return new Response(Response.OK, responseBody);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response upsert(final String key, final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(key.getBytes(UTF_8)), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response remove(final String key) throws IOException {
        dao.remove(ByteBuffer.wrap(key.getBytes(UTF_8)));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status(final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

}
