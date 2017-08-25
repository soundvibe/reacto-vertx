package net.soundvibe.reacto.vertx.events;

import io.reactivex.Flowable;
import io.reactivex.exceptions.MissingBackpressureException;
import io.reactivex.processors.*;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.logging.*;
import net.soundvibe.reacto.client.events.CommandHandler;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.errors.*;
import net.soundvibe.reacto.internal.InternalEvent;
import net.soundvibe.reacto.mappers.Mappers;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.vertx.server.handlers.WebSocketFrameHandler;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static net.soundvibe.reacto.mappers.Mappers.commandToBytes;
import static net.soundvibe.reacto.utils.WebUtils.*;

/**
 * @author OZY on 2015.11.23.
 */
public class VertxWebSocketCommandHandler implements CommandHandler, Closeable {

    private static final Logger log = LoggerFactory.getLogger(VertxWebSocketCommandHandler.class);
    public static final int INITIAL_CAPACITY = 10000;
    private final ServiceRecord serviceRecord;
    private final HttpClient httpClient;

    private final Map<String, FlowableProcessor<Event>> streams = new ConcurrentHashMap<>(INITIAL_CAPACITY);
    private CompletableFuture<WebSocket> webSocketStream;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public VertxWebSocketCommandHandler(ServiceRecord serviceRecord, Vertx vertx) {
        Objects.requireNonNull(serviceRecord, "serviceRecord cannot be null");
        Objects.requireNonNull(vertx, "vertx cannot be null");
        if (serviceRecord.type != ServiceType.WEBSOCKET)
            throw new IllegalStateException("Unexpected service type: expected WEBSOCKET, but got: " + serviceRecord.type);
        this.serviceRecord = serviceRecord;
        final HttpClientOptions httpClientOptions = new HttpClientOptions()
                //.setMaxPoolSize(32)
                .setUsePooledBuffers(true)
                .setTryUseCompression(true)
                .setReuseAddress(true)
                .setSsl(serviceRecord.location.asBoolean(ServiceRecord.LOCATION_SSL).orElse(false))
                .setKeepAlive(true)
                .setTcpKeepAlive(true)
                .setDefaultHost(serviceRecord.location.asString(ServiceRecord.LOCATION_HOST).orElse("localhost"))
                .setDefaultPort(serviceRecord.location.asInteger(ServiceRecord.LOCATION_PORT).orElse(80));
        this.httpClient = vertx.createHttpClient(httpClientOptions);
        createStream();
    }

    @Override
    public Flowable<Event> observe(Command command) {
        final String cmdId = command.id.toString();
        if (streams.size() > INITIAL_CAPACITY) return Flowable.error(new MissingBackpressureException("WebSocket Event Handler exceeded command limit"));

        final Flowable<Event> eventObservable = streams.compute(cmdId, (id, subject) ->
                subject != null ? subject : ReplayProcessor.create())
                .doOnCancel(() -> streams.remove(cmdId));

        if (isClosed()) {
            createStream();
        }

        return Flowable.fromFuture(webSocketStream, Schedulers.computation())
                .doOnNext(webSocket -> sendCommandForExecution(command, webSocket))
                .flatMap(webSocket -> eventObservable);
    }

    @Override
    public ServiceRecord serviceRecord() {
        return serviceRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertxWebSocketCommandHandler that = (VertxWebSocketCommandHandler) o;
        return Objects.equals(serviceRecord, that.serviceRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceRecord);
    }

    @Override
    public String name() {
        return serviceRecord.name;
    }


    @Override
    public void close() throws IOException {
        if (!webSocketStream.isCompletedExceptionally() && !webSocketStream.isCancelled()) {
            try {
                webSocketStream.get().close();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error when closing WebSocket: " + e);
            }
        }
    }

    private boolean isClosed() {
        return closed.get();
    }

    private synchronized void createStream() {
        this.webSocketStream = CompletableFuture.supplyAsync(this::connect, Executors.newCachedThreadPool());
    }

    private static void sendCommandForExecution(Command command, WebSocket webSocket) {
        if (log.isDebugEnabled()) {
            log.debug("Sending command for execution: " + command);
        }
        webSocket.writeBinaryMessage(Buffer.buffer(commandToBytes(command)));
    }

    private WebSocket connect() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<WebSocket> webSocketResult = new AtomicReference<>();
        log.info("Connecting to WebSocket...");
        httpClient.websocketStream(includeStartDelimiter(includeEndDelimiter(serviceRecord.location
                .asString(ServiceRecord.LOCATION_ROOT).orElse(serviceRecord.name))))
                .exceptionHandler(error -> {
                    countDownLatch.countDown();
                    doOnException(error);
                })
                .handler(webSocket -> {
                            webSocket
                                    .exceptionHandler(this::doOnException)
                                    .closeHandler(__ -> doOnClose())
                                    .frameHandler(new WebSocketFrameHandler(buffer -> handleEvent(buffer.getBytes())));
                            webSocketResult.set(webSocket);
                            countDownLatch.countDown();
                            closed.set(false);
                        }
                );
        try {
            countDownLatch.await(5L, TimeUnit.SECONDS);
            final WebSocket webSocket = webSocketResult.get();
            if (webSocket == null) {
                throw new CannotDiscoverService("Unable to connect to service's WebSocket: " + serviceRecord);
            }
            return webSocket;
        } catch (InterruptedException e) {
            throw new CannotDiscoverService("Interrupted when trying to connect to service's WebSocket: " + serviceRecord);
        }
    }

    private void doOnException(Throwable error) {
        log.error("WebSocket error: " + error);
        closed.set(true);
        failWithError(error);
    }

    private void doOnClose() {
        log.warn("WebSocket is closed for: " + serviceRecord);
        closed.set(true);
        failWithError(new ConnectionClosedUnexpectedly(
                "WebSocket connection closed without completion"));
    }

    private void handleEvent(byte[] eventBytes) {
        final InternalEvent internalEvent = Mappers.fromBytesToInternalEvent(eventBytes);
        final String cmdId = internalEvent.commandId().orElse("");
        if (log.isDebugEnabled()) {
            log.debug("InternalEvent [" + cmdId + "] is being handled: " + internalEvent.name + ": " + internalEvent.eventType);
        }
        final FlowableProcessor<Event> subject = streams.get(cmdId);
        if (subject == null) return;
        switch (internalEvent.eventType) {
            case NEXT: {
                subject.onNext(Mappers.fromInternalEvent(internalEvent));
                break;
            }
            case ERROR: {
                streams.remove(cmdId);
                subject.onError(internalEvent.error
                        .orElse(ReactiveException.from(new UnknownError("Unknown error from internalEvent: " + internalEvent))));

                break;
            }
            case COMPLETED: {
                streams.remove(cmdId);
                subject.onComplete();
                break;
            }
        }
    }

    private void failWithError(Throwable error) {
        streams.forEach((objectId, subject) -> subject.onError(error));
        streams.clear();
    }

}
