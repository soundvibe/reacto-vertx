package net.soundvibe.reacto.vertx.events;

import io.vertx.core.Vertx;
import io.vertx.servicediscovery.ServiceDiscovery;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.vertx.server.Factories;
import org.junit.Test;
import rx.observers.TestSubscriber;

import static org.junit.Assert.assertEquals;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxWebSocketEventHandlerTest {

    private final Vertx vertx = Factories.vertx();
    private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx);

    @Test
    public void shouldFailWhenHandlingReceivedEvent() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketEventHandler sut = new VertxWebSocketEventHandler(serviceRecord);

        final TestSubscriber<Event> testSubscriber = new TestSubscriber<>();

        sut.observe(Command.create("foo"))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNotCompleted();
        assertEquals(1, testSubscriber.getOnErrorEvents().size());
    }

    @Test
    public void shouldBeEqual() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("test-service", "/", "0.1", false, 8080),
                CommandRegistry.empty());
        VertxWebSocketEventHandler left = new VertxWebSocketEventHandler(serviceRecord);
        VertxWebSocketEventHandler right = new VertxWebSocketEventHandler(serviceRecord);

        assertEquals(left, right);

        assertEquals(left.hashCode(), right.hashCode());
    }
}