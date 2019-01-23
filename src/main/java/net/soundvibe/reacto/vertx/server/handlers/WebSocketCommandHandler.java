package net.soundvibe.reacto.vertx.server.handlers;

import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.functions.Functions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.logging.*;
import net.soundvibe.reacto.internal.InternalEvent;
import net.soundvibe.reacto.mappers.Mappers;
import net.soundvibe.reacto.server.CommandProcessor;
import net.soundvibe.reacto.types.*;

import java.util.Objects;
import java.util.function.Supplier;

import static net.soundvibe.reacto.mappers.Mappers.internalEventToBytes;
import static net.soundvibe.reacto.utils.WebUtils.*;

/**
 * @author Linas on 2015.12.03.
 */
public class WebSocketCommandHandler implements Handler<ServerWebSocket> {

    private final CommandProcessor commandProcessor;
    private final String root;

    private static final Logger log = LoggerFactory.getLogger(WebSocketCommandHandler.class);

    public WebSocketCommandHandler(CommandProcessor commandProcessor, String root) {
        Objects.requireNonNull(commandProcessor, "CommandProcessor cannot be null");
        Objects.requireNonNull(root, "Root cannot be null");
        this.commandProcessor = commandProcessor;
        this.root = root;
    }

    @Override
    public void handle(ServerWebSocket serverWebSocket) {
        if (!shouldHandle(serverWebSocket.path())) {
            log.warn("Rejecting WebSocket connection attempt to " + serverWebSocket.path());
            serverWebSocket.reject();
            return;
        }

        serverWebSocket
            .setWriteQueueMaxSize(Integer.MAX_VALUE)
            .frameHandler(new WebSocketFrameHandler(buffer -> {
                final Disposable subscription = Flowable.just(buffer.getBytes())
                        .map(Mappers::fromBytesToCommand)
                        .flatMap(command -> commandProcessor.process(command)
                                .materialize()
                                .doOnNext(eventNotification -> writeEventNotification(eventNotification, command, serverWebSocket))
                                .dematerialize(Functions.identity())
                        )
                        .subscribe(
                                event -> logDebug(() -> "Event was processed: " + event),
                                error -> log.error("Error when mapping from notification: " + error),
                                () -> logDebug(() -> "Command successfully processed")
                        );
                serverWebSocket
                        .exceptionHandler(exception -> {
                            log.error("ServerWebSocket exception: " + exception);
                            subscription.dispose();
                        })
                        .closeHandler(__ -> subscription.dispose())
                ;
            }));
    }

    private void logDebug(Supplier<String> text) {
        if (log.isDebugEnabled()) {
            log.debug(text.get());
        }
    }

    private static void writeEventNotification(Notification<Event> eventNotification, Command command, ServerWebSocket serverWebSocket) {
        if (eventNotification.isOnNext()) {
            writeOnNext(internalEventToBytes(InternalEvent.onNext(eventNotification.getValue(), command.id.toString())), serverWebSocket);
        } else if (eventNotification.isOnError()) {
            writeOnNext(internalEventToBytes(InternalEvent.onError(eventNotification.getError(), command.id.toString())), serverWebSocket);
        } else if (eventNotification.isOnComplete()) {
            writeOnNext(internalEventToBytes(InternalEvent.onCompleted(command.id.toString())), serverWebSocket);
        } else {
            throw new IllegalStateException("Unknown rx notification type: " + eventNotification);
        }
    }

    private static void writeOnNext(byte[] bytes, ServerWebSocket serverWebSocket) {
        serverWebSocket.writeBinaryMessage(Buffer.buffer(bytes));
    }

    private boolean shouldHandle(String path) {
        return root.equals(includeStartDelimiter(includeEndDelimiter(path)));
    }


}
