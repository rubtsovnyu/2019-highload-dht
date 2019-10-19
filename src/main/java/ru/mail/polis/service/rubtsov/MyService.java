package ru.mail.polis.service.rubtsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
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

import static com.google.common.base.Charsets.UTF_8;

public class MyService extends HttpServer implements Service {
    private final DAO dao;
    private final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(final int port,
                     @NotNull final DAO dao,
                     final int workersNumber) throws IOException {
        super(getConfig(port, workersNumber));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port, final int workersNumber) {
        final HttpServerConfig serverConfig = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        serverConfig.minWorkers = workersNumber;
        serverConfig.maxWorkers = workersNumber;
        return serverConfig;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status(final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Receives a request to an entity and respond depending on the method.
     *
     * @param id      Entity iD
     * @param request HTTP request
     * @param session HTTP session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        if (id == null || id.isEmpty()) {
            try {
                session.sendError(Response.BAD_REQUEST, "No id presented");
            } catch (IOException e) {
                logger.error("Error during response", e);
            }
            return;
        }
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    break;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody()));
                    break;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> remove(id));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Invalid method");
                    break;
            }
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Something went wrong...");
            } catch (IOException ex) {
                logger.error("Error on sending error O_o", ex);
            }
        }
    }

    /**
     * Receives a request for a range of values and returns it
     * by chunked transfer encoding mechanism.
     *
     * @param start   Start key
     * @param end     End key
     * @param request HTTP request
     * @param session HTTP session
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        try {
            if (start == null || start.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, "No start presented");
                return;
            }
            if (request.getMethod() != Request.METHOD_GET) {
                session.sendError(Response.METHOD_NOT_ALLOWED, "Invalid method");
                return;
            }
            if (end != null && end.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, "End should not be empty");
            }
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, null);
            } catch (IOException ex) {
                logger.error("Something went wrong during request processing", ex);
            }
        }
        try {
            final Iterator<Record> recordIterator = dao.range(
                    ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
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
        session.sendError(Response.BAD_REQUEST, "No.");
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StreamSession(socket, this);
    }

    private void executeAsync(final HttpSession httpSession, final Action action) {
        asyncExecute(() -> {
            try {
                httpSession.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    httpSession.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    logger.error("Error during request processing", ex);
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

}
