package net.soundvibe.reacto.vertx.server.handlers;

import io.reactivex.subscribers.TestSubscriber;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.servicediscovery.*;
import io.vertx.servicediscovery.types.HttpEndpoint;
import org.junit.Test;

/**
 * @author linas on 17.1.31.
 */
public class RxWrapTest {

    private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(Vertx.vertx());

    @Test
    public void shouldWrapVertxAsyncHandler() throws Exception {
        Record record = HttpEndpoint.createRecord("test", "localhost");

        TestSubscriber<Record> testSubscriber = new TestSubscriber<>();
        RxWrap.<Record>using(wrapper -> serviceDiscovery.publish(record, wrapper))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertComplete();
        testSubscriber.assertValue(record);
    }

    @Test
    public void shouldEmitError() throws Exception {
        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
        RxWrap.<Void>using(wrapper -> serviceDiscovery.unpublish("", wrapper))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNotComplete();
        testSubscriber.assertError(NoStackTraceThrowable.class);
    }
}