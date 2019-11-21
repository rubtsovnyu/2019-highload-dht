package ru.mail.polis.service.rubtsov;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import ru.mail.polis.dao.rubtsov.Item;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static ru.mail.polis.service.rubtsov.ServiceUtils.handleGetResponses;
import static ru.mail.polis.service.rubtsov.ServiceUtils.handlePutOrDeleteResponses;

public class MyService extends HttpServer implements Service {
    static final String TIMESTAMP_HEADER = "X-OK-Timestamp";
    private static final int TIMEOUT = 200;
    private static final String PROXY_HEADER = "X-OK-Proxy";
    private static final String ENTITY_PATH = "/v0/entity?id=";
    private static final int MIN_WORKERS = 8;
    private static final String FUTURE_ERROR_MSG = "Future trouble";

    private final DAO dao;
    private final Logger logger = LoggerFactory.getLogger(MyService.class);
    private final Topology<String> topology;
    private final Executor myWorkers;
    private final ReplicationFactor rf;
    private final HttpClient httpClient;

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
        logger.info("Service with port {} started", this.port);
        httpClient = HttpClient.newBuilder()
                .executor(myWorkers)
                .version(HttpClient.Version.HTTP_2)
                .build();
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
            sendInternalError(session);
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

    private static boolean isProxied(@NotNull final Request request) {
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
        final ReplicationFactor repFactor;
        repFactor = replicas == null ? this.rf : ReplicationFactor.from(replicas);
        if (repFactor.getAck() < 1
                || repFactor.getFrom() < repFactor.getAck()
                || repFactor.getFrom() > topology.size()) {
            try {
                session.sendError(Response.BAD_REQUEST, "Invalid replicas");
                return;
            } catch (IOException e) {
                logger.error("Error on sending 400", e);
                return;
            }
        }
        final boolean isProxied = isProxied(request);
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(() -> get(id, repFactor, isProxied, session));
                    break;
                case Request.METHOD_PUT:
                    executeAsync(() -> upsert(id, request.getBody(), repFactor, isProxied, session));
                    break;
                case Request.METHOD_DELETE:
                    executeAsync(() -> remove(id, repFactor, isProxied, session));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Invalid method");
                    break;
            }
        } catch (IOException e) {
            sendInternalError(session);
        }
    }

    @NotNull
    private static List<HttpRequest> getHttpRequests(final Topology<String> topology,
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

    private void get(final String id,
                     final ReplicationFactor rf,
                     final boolean proxy,
                     final HttpSession session) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final Iterator<Item> itemIterator = dao.latestIterator(key);
        if (proxy) {
            session.sendResponse(ServiceUtils.from(ValueUtils.from(key, itemIterator), true));
            return;
        }

        final List<String> nodes = topology.replicas(rf.getFrom(), key);

        final List<HttpRequest> requests = getHttpRequests(topology, id, HttpRequest.Builder::GET);

        final List<Value> values = new ArrayList<>();

        final List<CompletableFuture<HttpResponse<byte[]>>> futures = sendRequestsAndCollect(requests);

        final int ackNeeded = nodes.contains(topology.me()) ? rf.getAck() - 1 : rf.getAck();

        CompletableFuture.supplyAsync(() -> {
            if (nodes.contains(topology.me())) {
                return values.add(ValueUtils.from(key, itemIterator));
            } else {
                return false;
            }
        }, myWorkers)
                .thenComposeAsync(skip -> FutureUtils.getFutureResponses(futures, ackNeeded))
                .whenCompleteAsync((responses, fail) -> handleGetResponses(
                        nodes.contains(topology.me()),
                        rf.getAck(),
                        values,
                        responses,
                        session
                )).exceptionally(e -> {
            logger.error(FUTURE_ERROR_MSG, e);
            return null;
        });
    }

    @NotNull
    private List<CompletableFuture<HttpResponse<byte[]>>> sendRequestsAndCollect(final List<HttpRequest> requests) {
        return requests.stream()
                .map(request -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()))
                .collect(Collectors.toList());
    }

    private void upsert(final String id,
                        final byte[] valueArray,
                        final ReplicationFactor rf,
                        final boolean proxy,
                        final HttpSession session) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(valueArray);
        if (proxy) {
            dao.upsert(key, value);
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            return;
        }

        final List<String> nodes = topology.replicas(rf.getFrom(), key);

        final List<HttpRequest> requests = getHttpRequests(topology, id,
                (b) -> b.PUT(HttpRequest.BodyPublishers.ofByteArray(valueArray)));

        final List<CompletableFuture<HttpResponse<byte[]>>> futures = sendRequestsAndCollect(requests);

        final int ackNeeded = nodes.contains(topology.me()) ? rf.getAck() - 1 : rf.getAck();

        CompletableFuture.supplyAsync(() -> {
            if (nodes.contains(topology.me())) {
                try {
                    dao.upsert(key, value);
                } catch (IOException e) {
                    sendInternalError(session);
                    logger.error("Can't upsert to DAO {} : {}", key, value, e);
                }
            }
            return true;
        }, myWorkers)
                .thenComposeAsync(skip -> FutureUtils.getFutureResponses(futures, ackNeeded))
                .whenCompleteAsync((responses, fail) -> handlePutOrDeleteResponses(
                        nodes.contains(topology.me()),
                        rf.getAck(),
                        responses,
                        session,
                        201
                )).exceptionally(e -> {
            logger.error(FUTURE_ERROR_MSG, e);
            return null;
        });
    }

    private void remove(final String id,
                        final ReplicationFactor rf,
                        final boolean proxy,
                        final HttpSession session) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));
        if (proxy) {
            dao.remove(key);
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            return;
        }

        final List<String> nodes = topology.replicas(rf.getFrom(), key);

        final List<HttpRequest> requests = getHttpRequests(topology, id, HttpRequest.Builder::DELETE);

        final List<CompletableFuture<HttpResponse<byte[]>>> futures = sendRequestsAndCollect(requests);

        final int ackNeeded = nodes.contains(topology.me()) ? rf.getAck() - 1 : rf.getAck();

        CompletableFuture.supplyAsync(() -> {
            if (nodes.contains(topology.me())) {
                try {
                    dao.remove(key);
                } catch (IOException e) {
                    sendInternalError(session);
                    logger.error("Can't remove from DAO {}", key, e);
                }
            }
            return true;
        }, myWorkers)
                .thenComposeAsync(skip -> FutureUtils.getFutureResponses(futures, ackNeeded))
                .whenCompleteAsync((responses, fail) -> handlePutOrDeleteResponses(
                        nodes.contains(topology.me()),
                        rf.getAck(),
                        responses,
                        session,
                        202
                )).exceptionally(e -> {
            logger.error(FUTURE_ERROR_MSG, e);
            return null;
        });
    }

    private void sendInternalError(final HttpSession session) {
        try {
            session.sendError(Response.INTERNAL_ERROR, "Something went wrong...");
        } catch (IOException e) {
            logger.error("Can't send error", e);
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

    private void executeAsync(final Action action) {
        myWorkers.execute(() -> {
            try {
                action.act();
            } catch (IOException e) {
                logger.error("Error during request processing", e);
            }
        });
    }

    @FunctionalInterface
    interface Action {
        void act() throws IOException;
    }

}
