package net.soundvibe.reacto.vertx.agent;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.*;
import net.soundvibe.reacto.agent.AgentOptions;
import net.soundvibe.reacto.agent.AgentOptions.*;
import net.soundvibe.reacto.types.*;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class VertxSupervisorAgentTest {

    private final Vertx vertx = Vertx.vertx();
    private final VertxAgentSystem vertxAgentSystem = VertxAgentSystem.of(vertx);

    @Test
    public void shouldRestartOnErrorAndSelfHeal() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final TestAgentVerticle testAgent = new TestAgentVerticle(countDownLatch, AlwaysRestart.INSTANCE);
        vertxAgentSystem.run(() -> testAgent).blockingGet();
        countDownLatch.await(5, TimeUnit.SECONDS);

        assertEquals(1, testAgent.events.size());
    }

    @Test
    public void shouldNotRestartOnErrorWhenNeverRestartStrategy() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final TestAgentVerticle testAgent = new TestAgentVerticle(countDownLatch, NeverRestart.INSTANCE);
        vertxAgentSystem.run(() -> testAgent).blockingGet();
        countDownLatch.await(5, TimeUnit.SECONDS);

        assertEquals(0, testAgent.events.size());
    }

    @After
    public void tearDown() {
        vertx.close();
    }

    public class TestAgentVerticle extends AgentVerticle<Event> {
        private final AtomicBoolean toggle = new AtomicBoolean(true);

        public final List<Event> events = new ArrayList<>();
        private final CountDownLatch countDownLatch;

        public TestAgentVerticle(CountDownLatch countDownLatch, AgentOptions.AgentRestartStrategy restartStrategy) {
            super("testAgent", VertxAgentOptions.from(new DeploymentOptions()
                    .setInstances(1)
                    .setHa(true))
                    .setClusterInstances(() -> 4)
                    //.setClusterInstances(4)
                    .setAgentRestartStrategy(restartStrategy));
            this.countDownLatch = countDownLatch;
        }

        @Override
        public Flowable<Event> run() {
            return Flowable.just(toggle.getAndSet(false))
                    .flatMap(shouldContinue -> shouldContinue ?
                            Flowable.error(new IllegalArgumentException("Test error")) :
                            Flowable.just(TypedEvent.create("test", MetaData.empty())))
                    .doOnNext(events::add)
                    .doOnComplete(countDownLatch::countDown)
                    .subscribeOn(Schedulers.computation());
        }

        @Override
        public int version() {
            return 1;
        }
    }
}