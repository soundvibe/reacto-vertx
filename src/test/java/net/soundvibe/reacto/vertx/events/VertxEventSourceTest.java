package net.soundvibe.reacto.vertx.events;

import io.vertx.core.Vertx;
import net.soundvibe.reacto.vertx.server.Factories;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * @author OZY on 2017.01.24.
 */
public class VertxEventSourceTest {

    private final Vertx vertx = Factories.vertx();

    @Test
    public void shouldFailWhenTryingToConnect() throws Exception {
        AtomicReference<Throwable> hasError = new AtomicReference<>();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        VertxEventSource sut = new VertxEventSource(vertx, "http://localhost:8181/reacto.stream")
                .onError(throwable -> {
                    hasError.set(throwable);
                    countDownLatch.countDown();
                })
                .onMessage(s -> countDownLatch.countDown())
                .onOpen(countDownLatch::countDown);
        sut.open();
        countDownLatch.await(5000L, TimeUnit.MILLISECONDS);
        assertNotNull(hasError.get());
    }

}