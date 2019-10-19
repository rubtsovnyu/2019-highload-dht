package ru.mail.polis.service.rubtsov;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;

public class MyService extends HttpServer implements Service {
    private final DAO dao;
    private final Executor myWorkers;
    private final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(final int port,
                     @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.myWorkers = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("worker-%d").build());
    }

    private static HttpServerConfig getConfig(final int port) {
        final HttpServerConfig serverConfig = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status(final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    private void entity(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No id presented");
            return;
        }
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    return;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody()));
                    return;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> remove(id));
                    return;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Invalid method");
            }
        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, "Something went wrong...");
        }
    }

    private void entities(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start presented");
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Invalid method");
            return;
        }
        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }
        final Iterator<Record> recordIterator = dao.range(
                ByteBuffer.wrap(start.getBytes(UTF_8)),
                end == null ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
        try {
            ((StreamSession) session).stream(recordIterator);
        } catch (IOException e) {
            logger.error("Error during stream of range from {} to {}", start, end, e);
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

    @Override
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "Invalid path");
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StreamSession(socket, this);
    }

    private void executeAsync(final HttpSession httpSession, final Action action) {
        myWorkers.execute(() -> {
            try {
                httpSession.sendResponse(action.act());
            } catch (Exception e) {
                try {
                    httpSession.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    logger.error("Error during request process", ex);
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

}
