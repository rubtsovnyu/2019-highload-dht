package ru.mail.polis.service.rubtsov;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.rubtsov.Item;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;

public class MyService extends HttpServer implements Service {
    private static final int MIN_WORKERS = 4;
    private static final String PROXY_HEADER = "X-OK-Proxy: true";
    private static final String ENTITY_PATH = "/v0/entity?id=";
    static final String TIMESTAMP_HEADER = "X-OK-Timestamp: ";

    private final DAO dao;
    private final Logger logger = LoggerFactory.getLogger(MyService.class);
    private final Topology<String> topology;
    private final Map<String, HttpClient> clientPool;
    private final Executor myWorkers;
    private final ReplicationFactor rf;

    /**
     * Create new service.
     * @param port Service port
     * @param dao Service DAO
     * @param topology Topology
     * @throws IOException Sometimes something went wrong
     */
    public MyService(final int port,
                     @NotNull final DAO dao,
                     @NotNull final Topology<String> topology) throws IOException {
        super(getConfig(port));
        logger.info("Starting service with port {}...", port);
        this.topology = topology;
        this.dao = dao;
        rf = ReplicationFactor.quorum(topology.size());
        final int workersNumber = Math.max(
                Runtime.getRuntime().availableProcessors(), MIN_WORKERS);
        myWorkers = Executors.newFixedThreadPool(
                workersNumber,
                new ThreadFactoryBuilder().setNameFormat("worker-%d").build());
        this.clientPool = new HashMap<>();
        for (final String node :
                topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            clientPool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
        logger.info("Service with port {} started", this.port);
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

    private static boolean isProxied(@NotNull Request request) {
        return request.getHeader(PROXY_HEADER) != null;
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
            @Param("replicas") final String replicas,
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
        final boolean proxied = isProxied(request);
        final ReplicationFactor repFactor;
        repFactor = replicas == null ? this.rf : ReplicationFactor.from(replicas);
        if (repFactor.getAck() < 1 || repFactor.getFrom() < repFactor.getAck() || repFactor.getFrom() > topology.size()) {
            try {
                session.sendError(Response.BAD_REQUEST, "Invalid replicas");
                return;
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Something went wrong...");
                } catch (IOException ex) {
                    logger.error("Error on sending error O_o", ex);
                    return;
                }
            }
        }
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id, repFactor, proxied));
                    break;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody(), repFactor, proxied));
                    break;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> remove(id, repFactor, proxied));
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

    private Response get(final String id,
                         final ReplicationFactor rf,
                         final boolean proxy) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final Iterator<Item> itemIterator = dao.latestIterator(key);
        if (proxy) {
            return ServiceUtils.from(ValueUtils.from(key, itemIterator), true);
        }

        final String[] nodes = topology.replicas(rf.getFrom(), key);
        final List<Value> responseValues = new ArrayList<>();
        int asks = 0;

        for (final String node :
                nodes) {
            if (topology.isMe(node)) {
                responseValues.add(ValueUtils.from(key, itemIterator));
                asks++;
            } else {
                try {
                    final Response response = clientPool.get(node)
                            .get(ENTITY_PATH + id, PROXY_HEADER);
                    asks++;
                    responseValues.add(ValueUtils.from(response));
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can't get answer from {} for get an item", node, e);
                }
            }
        }
        if (asks >= rf.getAck()) {
            return ServiceUtils.from(ValueUtils.merge(responseValues), false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response upsert(final String id,
                            final byte[] valueArray,
                            final ReplicationFactor rf,
                            final boolean proxy) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(valueArray);
        if (proxy) {
            dao.upsert(key, value);
            return new Response(Response.CREATED, Response.EMPTY);
        }

        final String[] nodes = topology.replicas(rf.getFrom(), key);
        int asks = 0;

        for (final String node :
                nodes) {
            if (topology.isMe(node)) {
                dao.upsert(key, value);
                asks++;
            } else {
                try {
                    final Response response = clientPool.get(node)
                            .put(ENTITY_PATH + id, valueArray, PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        asks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can't get answer from {} for upsert an item", node, e);
                }
            }
        }
        if (asks >= rf.getAck()) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response remove(final String id,
                            final ReplicationFactor rf,
                            final boolean proxy) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        if (proxy) {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }

        final String[] nodes = topology.replicas(rf.getFrom(), key);
        int asks = 0;

        for (final String node :
                nodes) {
            if (topology.isMe(node)) {
                dao.remove(key);
                asks++;
            } else {
                try {
                    final Response response = clientPool.get(node)
                            .delete(ENTITY_PATH + id, PROXY_HEADER);
                    if (response.getStatus() == 202) {
                        asks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can't get answer from {} for remove an item", node, e);
                }
            }
        }
        if (asks >= rf.getAck()) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
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

    @Override
    public synchronized void stop() {
        logger.info("Stopping service with port {}...", port);
        super.stop();
        try {
            dao.close();
        } catch (IOException e) {
            logger.error("Can't stop DAO, port {}", port);
        }
        logger.info("Service with port {} stopped", port);
    }

    private void executeAsync(final HttpSession httpSession, final Action action) {
        myWorkers.execute(() -> {
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
