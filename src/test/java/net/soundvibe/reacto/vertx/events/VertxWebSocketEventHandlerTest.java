package net.soundvibe.reacto.vertx.events;

import io.vertx.core.Vertx;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import org.junit.Test;
import rx.observers.TestSubscriber;

import static org.junit.Assert.assertEquals;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxWebSocketEventHandlerTest {

    private final Vertx vertx = Vertx.vertx();

    @Test
    public void shouldFailWhenHandlingReceivedEvent() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketEventHandler sut = new VertxWebSocketEventHandler(serviceRecord, vertx);

        final TestSubscriber<Event> testSubscriber = new TestSubscriber<>();

        sut.observe(Command.create("foo"))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNotCompleted();
        assertEquals(1, testSubscriber.getOnErrorEvents().size());
        sut.close();
    }

    @Test
    public void shouldBeEqual() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketEventHandler left = new VertxWebSocketEventHandler(serviceRecord, vertx);
        VertxWebSocketEventHandler right = new VertxWebSocketEventHandler(serviceRecord, vertx);

        assertEquals(left, right);

        assertEquals(left.hashCode(), right.hashCode());

        left.close();
        right.close();
    }
}