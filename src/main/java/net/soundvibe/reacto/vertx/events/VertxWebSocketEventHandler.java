package net.soundvibe.reacto.vertx.events;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.logging.*;
import net.soundvibe.reacto.client.events.EventHandler;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.errors.*;
import net.soundvibe.reacto.internal.InternalEvent;
import net.soundvibe.reacto.mappers.Mappers;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.vertx.server.Factories;
import net.soundvibe.reacto.vertx.server.handlers.WebSocketFrameHandler;
import rx.*;

import java.util.Objects;
import java.util.function.Function;

import static net.soundvibe.reacto.utils.WebUtils.*;

/**
 * @author OZY on 2015.11.23.
 */
public class VertxWebSocketEventHandler implements EventHandler, Function<ServiceRecord, EventHandler> {

    private static final Logger log = LoggerFactory.getLogger(VertxWebSocketEventHandler.class);
    private final ServiceRecord serviceRecord;
    private final HttpClient httpClient;

    public VertxWebSocketEventHandler(ServiceRecord serviceRecord) {
        Objects.requireNonNull(serviceRecord, "serviceRecord cannot be null");
        if (serviceRecord.type != ServiceType.WEBSOCKET)
            throw new IllegalStateException("Unexpected service type: expected WEBSOCKET, but got: " + serviceRecord.type);
        this.serviceRecord = serviceRecord;
        final HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setSsl(serviceRecord.location.asBoolean(ServiceRecord.LOCATION_SSL).orElse(false))
                .setKeepAlive(true)
                .setTcpKeepAlive(true)
                .setDefaultHost(serviceRecord.location.asString(ServiceRecord.LOCATION_HOST).orElse("localhost"))
                .setDefaultPort(serviceRecord.location.asInteger(ServiceRecord.LOCATION_PORT).orElse(80));
        this.httpClient = Factories.vertx().createHttpClient(httpClientOptions);
    }

    @Override
    public Observable<Event> observe(Command command) {
        return Observable.just(serviceRecord)
                .map(rec -> httpClient.websocketStream(includeStartDelimiter(includeEndDelimiter(rec.name))))
                .concatMap(webSocketStream -> observe(webSocketStream, command)
                        .onBackpressureBuffer());
    }

    @Override
    public ServiceRecord serviceRecord() {
        return serviceRecord;
    }

    public static EventHandler create(ServiceRecord serviceRecord) {
        return new VertxWebSocketEventHandler(serviceRecord);
    }

    @Override
    public EventHandler apply(ServiceRecord serviceRecord) {
        return create(serviceRecord);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertxWebSocketEventHandler that = (VertxWebSocketEventHandler) o;
        return Objects.equals(serviceRecord, that.serviceRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceRecord);
    }

    private static void checkForEvents(WebSocket webSocket, Subscriber<? super Event> subscriber) {
        webSocket
                .frameHandler(new WebSocketFrameHandler(buffer -> {
                    try {
                        if (!subscriber.isUnsubscribed()) {
                            handleEvent(Mappers.fromBytesToInternalEvent(buffer.getBytes()), subscriber);
                        }
                    } catch (Throwable e) {
                        subscriber.onError(e);
                    }
                }));
    }

    @SuppressWarnings("unchecked")
    private static void handleEvent(InternalEvent internalEvent, Subscriber<? super Event> subscriber) {
        log.debug("InternalEvent has been received and is being handled: " + internalEvent);
        switch (internalEvent.eventType) {
            case NEXT: {
                subscriber.onNext(Mappers.fromInternalEvent(internalEvent));
                break;
            }
            case ERROR: {
                if (!subscriber.isUnsubscribed()) {
                    subscriber.onError(internalEvent.error
                            .orElse(ReactiveException.from(new UnknownError("Unknown error from internalEvent: " + internalEvent))));
                }
                break;
            }
            case COMPLETED: {
                if (!subscriber.isUnsubscribed()) {
                    subscriber.onCompleted();
                }
                break;
            }
        }
    }

    public static Observable<Event> observe(WebSocketStream webSocketStream, Command command) {
        return Observable.create(subscriber -> {
            try {
                webSocketStream
                        .exceptionHandler(subscriber::onError)
                        .handler(webSocket -> {
                            try {
                                webSocket.closeHandler(__ -> {
                                    if (!subscriber.isUnsubscribed()) {
                                        subscriber.onError(new ConnectionClosedUnexpectedly(
                                                "WebSocket connection closed without completion for command: " + command));
                                    }
                                }).exceptionHandler(subscriber::onError);
                                checkForEvents(webSocket, subscriber);
                                sendCommandToExecutor(command, webSocket);
                            } catch (Throwable e) {
                                subscriber.onError(e);
                            }
                        });
            } catch (Throwable e) {
                subscriber.onError(e);
            }
        });
    }

    private static void sendCommandToExecutor(Command command, WebSocket webSocket) {
        log.debug("Sending command to executor: " + command);
        final byte[] bytes = Mappers.commandToBytes(command);
        webSocket.writeBinaryMessage(Buffer.buffer(bytes));
    }

    @Override
    public String name() {
        return serviceRecord.name;
    }


}
