package ru.mail.polis.service.rubtsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.google.common.base.Charsets.UTF_8;

class StreamSession extends HttpSession {
    private static final byte[] CRLF = "\r\n".getBytes(UTF_8);
    private static final byte LF = '\n';
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(UTF_8);
    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding: chunked";

    private Iterator<Record> recordIterator;

    StreamSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    private static byte[] toByteArray(@NotNull final ByteBuffer byteBuffer) {
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    void stream(@NotNull Iterator<Record> recordIterator) throws IOException {
        this.recordIterator = recordIterator;

        final Response response = new Response(Response.OK);
        response.addHeader(TRANSFER_ENCODING_HEADER);
        writeResponse(response, false);

        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();

        next();
    }

    private void next() throws IOException {
        while (recordIterator.hasNext() && queueHead == null) {
            final Record record = recordIterator.next();
            final byte[] key = toByteArray(record.getKey());
            final byte[] value = toByteArray(record.getValue());
            final int recordLength = key.length + 1 + value.length;
            final String size = Integer.toHexString(recordLength);

            final int chunkLength = size.length() + 2 + recordLength + 2;

            final byte[] chunk = new byte[chunkLength];
            final ByteBuffer byteBuffer = ByteBuffer.wrap(chunk);
            byteBuffer.put(size.getBytes(UTF_8))
                    .put(CRLF)
                    .put(key)
                    .put(LF)
                    .put(value)
                    .put(CRLF);
            write(chunk, 0, chunkLength);
        }

        if (!recordIterator.hasNext()) {
            write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    try {
                        server.handleRequest(handling, this);
                    } catch (IOException e) {
                        log.error("Can't process next request: " + handling, e);
                    }
                }

            }
        }
    }
}
