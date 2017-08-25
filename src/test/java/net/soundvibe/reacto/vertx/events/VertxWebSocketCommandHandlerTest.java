package net.soundvibe.reacto.vertx.events;

import io.reactivex.subscribers.TestSubscriber;
import io.vertx.core.Vertx;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxWebSocketCommandHandlerTest {

    private final Vertx vertx = Vertx.vertx();

    @Test
    public void shouldFailWhenHandlingReceivedEvent() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketCommandHandler sut = new VertxWebSocketCommandHandler(serviceRecord, vertx);

        final TestSubscriber<Event> testSubscriber = new TestSubscriber<>();

        sut.observe(Command.create("foo"))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNotComplete();
        assertEquals(1, testSubscriber.errorCount());
        sut.close();
    }

    @Test
    public void shouldBeEqual() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketCommandHandler left = new VertxWebSocketCommandHandler(serviceRecord, vertx);
        VertxWebSocketCommandHandler right = new VertxWebSocketCommandHandler(serviceRecord, vertx);

        assertEquals(left, right);

        assertEquals(left.hashCode(), right.hashCode());

        left.close();
        right.close();
    }
}