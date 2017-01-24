package net.soundvibe.reacto.vertx.events;

import io.vertx.core.Vertx;
import io.vertx.servicediscovery.ServiceDiscovery;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.vertx.server.Factories;
import org.junit.Test;
import rx.observers.TestSubscriber;

import static org.junit.Assert.assertEquals;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxDiscoverableEventHandlerTest {

    private final Vertx vertx = Factories.vertx();
    private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx);

    @Test
    public void shouldFailWhenHandlingReceivedEvent() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint("test-service", 8080, "/", "0.1");
        VertxDiscoverableEventHandler sut = new VertxDiscoverableEventHandler(serviceRecord, serviceDiscovery);

        final TestSubscriber<Event> testSubscriber = new TestSubscriber<>();

        sut.observe(Command.create("foo"))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNotCompleted();
        assertEquals(1, testSubscriber.getOnErrorEvents().size());
    }

    @Test
    public void shouldBeEqual() throws Exception {
        final ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint("test-service", 8080, "/", "0.1");
        VertxDiscoverableEventHandler left = new VertxDiscoverableEventHandler(serviceRecord, serviceDiscovery);
        VertxDiscoverableEventHandler right = new VertxDiscoverableEventHandler(serviceRecord, serviceDiscovery);
        assertEquals(left, right);

        assertEquals(left.hashCode(), right.hashCode());
    }
}