package ru.mail.polis.service.rubtsov;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

class FutureUtils {
    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    private FutureUtils() {
    }

    static CompletableFuture<List<HttpResponse<byte[]>>> getFutureResponses(
            @NotNull final List<CompletableFuture<HttpResponse<byte[]>>> futureList,
            final int ack) {
        if (ack < 1) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        final AtomicInteger fails = new AtomicInteger();
        final int permissibleFails = futureList.size() - ack;
        final List<HttpResponse<byte[]>> httpResponses = new CopyOnWriteArrayList<>();
        final CompletableFuture<List<HttpResponse<byte[]>>> results = new CompletableFuture<>();

        final BiConsumer<HttpResponse<byte[]>, Throwable> handler = (value, fail) -> {
            if (value != null) {
                if (!results.isDone()) {
                    httpResponses.add(value);
                    if (httpResponses.size() >= ack) {
                        results.complete(httpResponses);
                    }
                }
            } else if (fail != null) {
                if (fails.incrementAndGet() > permissibleFails) {
                    results.complete(httpResponses);
                }
            }
        };

        for (final CompletableFuture<HttpResponse<byte[]>> f :
                futureList) {
            f.whenCompleteAsync(handler)
                    .exceptionally(e -> {
                        logger.error("Future trouble: {}", e.getCause().getMessage());
                        return null;
                    });
        }
        return results;
    }

}
