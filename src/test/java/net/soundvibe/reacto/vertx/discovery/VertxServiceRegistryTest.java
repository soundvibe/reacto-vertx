package net.soundvibe.reacto.vertx.discovery;

import io.reactivex.subscribers.TestSubscriber;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.servicediscovery.*;
import io.vertx.servicediscovery.Status;
import io.vertx.servicediscovery.types.HttpEndpoint;
import net.soundvibe.reacto.client.events.CommandHandlerRegistry;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.errors.CannotDiscoverService;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.server.ServiceOptions;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.utils.WebUtils;
import net.soundvibe.reacto.vertx.events.VertxWebSocketCommandHandler;
import net.soundvibe.reacto.vertx.types.DemoServiceRegistryMapper;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxServiceRegistryTest {

    private static final String TEST_SERVICE = "testService";
    private static final String ROOT = "/test/";

    private final Vertx vertx = Vertx.vertx();
    private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx);

    private final CommandHandlerRegistry commandHandlerRegistry = CommandHandlerRegistry.Builder.create()
            .register(ServiceType.WEBSOCKET, serviceRecord -> new VertxWebSocketCommandHandler(serviceRecord, vertx))
            .build();

    private final VertxServiceRegistry sut = new VertxServiceRegistry(
            commandHandlerRegistry,
            serviceDiscovery,
            new DemoServiceRegistryMapper(),
            ServiceRecord.createWebSocketEndpoint(
                    new ServiceOptions(TEST_SERVICE, ROOT, "0.1", false, 8181),
                    Collections.emptyList()));

    @Test
    public void shouldStartDiscovery() throws Exception {
        assertDiscoveredServices(0);

        TestSubscriber<Any> recordTestSubscriber = new TestSubscriber<>();
        sut.register()
                .subscribe(recordTestSubscriber);

        recordTestSubscriber.awaitTerminalEvent();
        recordTestSubscriber.assertNoErrors();
        recordTestSubscriber.assertValueCount(1);

        assertDiscoveredServices(1);
    }

    @Test
    public void shouldCloseDiscovery() throws Exception {
        shouldStartDiscovery();
        TestSubscriber<Any> closeSubscriber = new TestSubscriber<>();
        sut.unregister().subscribe(closeSubscriber);
        closeSubscriber.awaitTerminalEvent();
        closeSubscriber.assertNoErrors();
        closeSubscriber.assertValueCount(1);

        assertDiscoveredServices(0);
    }

    @Test
    public void shouldRemoveDownRecords() throws Exception {
        shouldStartDiscovery();

        final Record record = HttpEndpoint.createRecord(
                TEST_SERVICE,
                WebUtils.getLocalAddress(),
                8888,
                ROOT);

        serviceDiscovery.publish(record, event -> {});
        Thread.sleep(200L);
        serviceDiscovery.update(record.setStatus(Status.DOWN), event -> {});
        Thread.sleep(100L);

        List<Record> recordList = getRecords(Status.DOWN);
        assertEquals("Should be one service down", 1, recordList.size());
        TestSubscriber<Record> testSubscriber = new TestSubscriber<>();

        sut.cleanServices().subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);

        List<Record> records = getRecords(Status.DOWN);
        assertEquals("Should be no services down", 0, records.size());
    }

    @Test
    public void shouldEmitErrorWhenFindIsUnableToGetServices() throws Exception {
        TestSubscriber<Event> subscriber = new TestSubscriber<>();
        TestSubscriber<Any> recordTestSubscriber = new TestSubscriber<>();
        TestSubscriber<Any> closeSubscriber = new TestSubscriber<>();
        final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(Vertx.vertx());
        final ServiceRecord record = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("testService", "test/", "0.1", false, 8123),
                Collections.emptyList());
        final VertxServiceRegistry serviceRegistry = new VertxServiceRegistry(
                commandHandlerRegistry, serviceDiscovery,
                new JacksonMapper(Json.mapper), record);


        serviceRegistry.register()
                .subscribe(recordTestSubscriber);

        recordTestSubscriber.awaitTerminalEvent();
        recordTestSubscriber.assertNoErrors();

        serviceRegistry.unregister().subscribe(closeSubscriber);
        closeSubscriber.awaitTerminalEvent();
        closeSubscriber.assertNoErrors();

        serviceRegistry.execute(Command.create("sdsd"))
                .subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertError(CannotDiscoverService.class);
    }

    @Test
    public void shouldMapStatus() throws Exception {
        assertEquals(net.soundvibe.reacto.discovery.types.Status.UP, VertxServiceRegistry.mapStatus(io.vertx.servicediscovery.Status.UP));
        assertEquals(net.soundvibe.reacto.discovery.types.Status.DOWN, VertxServiceRegistry.mapStatus(io.vertx.servicediscovery.Status.DOWN));
        assertEquals(net.soundvibe.reacto.discovery.types.Status.OUT_OF_SERVICE, VertxServiceRegistry.mapStatus(io.vertx.servicediscovery.Status.OUT_OF_SERVICE));
        assertEquals(net.soundvibe.reacto.discovery.types.Status.UNKNOWN, VertxServiceRegistry.mapStatus(io.vertx.servicediscovery.Status.UNKNOWN));
    }

    @Test
    public void shouldMapDefaultServiceType() throws Exception {
        final ServiceType actual = VertxServiceRegistry.mapServiceType(new Record()
                .setType("UNKNOWN"));
        assertEquals(ServiceType.WEBSOCKET, actual);
    }

    private List<Record> getRecords(Status status) throws InterruptedException {
        List<Record> recordList = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        serviceDiscovery.getRecords(record -> record.getStatus().equals(status), true, event -> {
            if (event.succeeded()) {
                recordList.addAll(event.result());
            }
            countDownLatch.countDown();
        });
        countDownLatch.await();
        return recordList;
    }

    private void assertDiscoveredServices(int count) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);

        serviceDiscovery.getRecords(record -> record.getName().equals(TEST_SERVICE), true,
                event -> {
                    if (event.succeeded()) {
                        counter.set(event.result().size());
                    }
                    countDownLatch.countDown();
                });

        countDownLatch.await();

        assertEquals(count, counter.get());
    }

}