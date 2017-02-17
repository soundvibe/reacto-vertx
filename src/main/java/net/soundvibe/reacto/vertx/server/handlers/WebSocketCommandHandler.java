package net.soundvibe.reacto.vertx.server.handlers;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.logging.*;
import net.soundvibe.reacto.internal.InternalEvent;
import net.soundvibe.reacto.mappers.Mappers;
import net.soundvibe.reacto.server.CommandProcessor;
import net.soundvibe.reacto.types.*;
import rx.*;

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
            serverWebSocket.reject();
            return;
        }

        serverWebSocket
            .setWriteQueueMaxSize(Integer.MAX_VALUE)
            .frameHandler(new WebSocketFrameHandler(buffer -> {
                final Subscription subscription = Observable.just(buffer.getBytes())
                        .map(Mappers::fromBytesToCommand)
                        .flatMap(command -> commandProcessor.process(command)
                                .materialize()
                                .doOnNext(eventNotification -> writeEventNotification(eventNotification, command, serverWebSocket))
                                .dematerialize()
                        )
                        .subscribe(
                                event -> logDebug(() -> "Event was processed: " + event),
                                error -> log.error("Error when mapping from notification: " + error),
                                () -> logDebug(() -> "Command successfully processed")
                        );
                serverWebSocket
                        .exceptionHandler(exception -> {
                            log.error("ServerWebSocket exception: " + exception);
                            subscription.unsubscribe();
                        })
                        .closeHandler(__ -> subscription.unsubscribe())
                ;
            }));
    }

    private void logDebug(Supplier<String> text) {
        if (log.isDebugEnabled()) {
            log.debug(text.get());
        }
    }

    private static void writeEventNotification(Notification<Event> eventNotification, Command command, ServerWebSocket serverWebSocket) {
        switch (eventNotification.getKind()) {
            case OnNext:
                writeOnNext(internalEventToBytes(InternalEvent.onNext(eventNotification.getValue(), command.id.toString())), serverWebSocket);
                break;
            case OnError:
                writeOnNext(internalEventToBytes(InternalEvent.onError(eventNotification.getThrowable(), command.id.toString())), serverWebSocket);
                break;
            case OnCompleted:
                writeOnNext(internalEventToBytes(InternalEvent.onCompleted(command.id.toString())), serverWebSocket);
                break;
            default: throw new IllegalStateException("Unknown rx notification type: " + eventNotification);
        }
    }

    private static void writeOnNext(byte[] bytes, ServerWebSocket serverWebSocket) {
        serverWebSocket.writeBinaryMessage(Buffer.buffer(bytes));
    }

    private boolean shouldHandle(String path) {
        return root.equals(includeStartDelimiter(includeEndDelimiter(path)));
    }


}
