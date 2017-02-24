package net.soundvibe.reacto.vertx.server.handlers;

import com.netflix.hystrix.*;
import com.netflix.hystrix.metric.consumer.HystrixDashboardStream;
import io.vertx.core.Vertx;
import io.vertx.core.http.*;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.servicediscovery.ServiceDiscovery;
import net.soundvibe.reacto.client.events.*;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.vertx.discovery.VertxServiceRegistry;
import net.soundvibe.reacto.vertx.events.*;
import net.soundvibe.reacto.vertx.server.VertxServer;
import org.junit.*;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static net.soundvibe.reacto.vertx.server.VertxServer.*;
import static org.junit.Assert.*;

/**
 * @author OZY on 2017.01.24.
 */
public class HystrixEventStreamHandlerTest {

    private static final int PORT = 8282;
    public static final String SUFFIX = "}" + "\n\n";
    public static final String PREFIX = "{";
    public static final String URL_HYSTRIX = "http://localhost:8282/test/" + HYSTRIX_STREAM_PATH;
    public static final String URL_REACTO = "http://localhost:8282/test/" + REACTO_STREAM_PATH;
    private VertxServer vertxServer;
    private VertxServiceRegistry serviceRegistry;
    private final Vertx vertx = Vertx.vertx();
    private HttpClient httpClient = null;
    private final AtomicInteger count = new AtomicInteger(0);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final EventHandlerRegistry eventHandlerRegistry = EventHandlerRegistry.Builder.create()
            .register(ServiceType.WEBSOCKET, VertxWebSocketEventHandler::new)
            .build();

    @Before
    public void setUp() throws Exception {
        final ServiceOptions serviceOptions = new ServiceOptions("test", "test", "0.1",false, PORT);
        final CommandRegistry commandRegistry = CommandRegistry.of("demo", o -> Observable.just(Event.create("foo"), Event.create("bar")));
        final Router router = Router.router(vertx);
        serviceRegistry = new VertxServiceRegistry(eventHandlerRegistry,
                ServiceDiscovery.create(vertx),
                new JacksonMapper(Json.mapper),
                ServiceRecord.createWebSocketEndpoint(serviceOptions, commandRegistry));
        vertxServer = new VertxServer(serviceOptions,
                router, vertx.createHttpServer(new HttpServerOptions().setPort(PORT)),
                commandRegistry,
                serviceRegistry);

        vertxServer.start().toBlocking().subscribe();
    }

    @After
    public void tearDown() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
        vertxServer.stop().toBlocking().subscribe();
    }

    @Test
    public void shouldWriteSomeDataWhenCommandIsExecuted() throws Exception {
        createEventSource(URL_HYSTRIX);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        new FooCommand("foo").toObservable()
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValue("foo");

        await();
        assertTrue("Should received at least one message", count.get() > 0);
    }

    @Test
    public void shouldGetDashboardData() throws Exception {
        TestSubscriber<HystrixDashboardStream.DashboardData> testSubscriber = new TestSubscriber<>();
        HystrixDashboardStream.getInstance().observe()
                .subscribe(testSubscriber);

        TestSubscriber<String> subscriber = new TestSubscriber<>();
        new FooCommand("foo").toObservable().subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertCompleted();

        testSubscriber.awaitTerminalEventAndUnsubscribeOnTimeout(1000L, TimeUnit.MILLISECONDS);
        final List<HystrixDashboardStream.DashboardData> onNextEvents = testSubscriber.getOnNextEvents();
        System.out.println(onNextEvents);
        assertFalse(onNextEvents.isEmpty());
        final HystrixDashboardStream.DashboardData dashboardData = onNextEvents.get(0);
        final String commandJson = HystrixEventStreamHandler.getCommandJson(dashboardData.getCommandMetrics().stream().findAny().orElseThrow(RuntimeException::new));
        assertTrue(commandJson.startsWith("{") && commandJson.endsWith("}"));
    }

    private void await() {
        try {
            countDownLatch.await(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //
        }
    }

    private void createEventSource(String url) {
        final EventSource eventSource = new VertxEventSource(vertx, url);
        eventSource.open();
        eventSource.onMessage(this::assertDataMessage);
        eventSource.onError(this::assertError);
    }

    private void assertDataMessage(String json) {
        count.incrementAndGet();
        assertTrue("Should start with { but was: " + json, json.startsWith(PREFIX));
        assertTrue("Should end with } but was: " + json.substring(json.length() - 3), json.endsWith(SUFFIX));
        countDownLatch.countDown();
    }

    private void assertError(Throwable error) {
        countDownLatch.countDown();
        fail(error.toString());
    }




    private class FooCommand extends HystrixObservableCommand<String> {
        private final String value;

        FooCommand(String value) {
            super(HystrixCommandGroupKey.Factory.asKey("foo"));
            this.value = value;
        }

        @Override
        protected Observable<String> construct() {
            return Observable.just(value);
        }
    }

}