package net.soundvibe.reacto.vertx.agent;

import io.reactivex.Flowable;
import io.vertx.core.*;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.core.logging.LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME;
import static net.soundvibe.reacto.vertx.agent.VertxSupervisorAgent.findRunningAgents;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ClusteredVertxSupervisorAgentTest {

    private ClusterManager clusterManager;

    private VertxAgentSystem agentSystem1;
    private VertxAgentSystem agentSystem2;

    static {
        System.setProperty(LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
    }

    @Before
    public void setUp() {
        agentSystem1 = VertxAgentSystem.of(vertxOptions());
        agentSystem2 = VertxAgentSystem.of(vertxOptions());

        clusterManager = agentSystem1.clusterManager()
                .orElseThrow(RuntimeException::new);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (agentSystem1 != null) {
            leaveCluster(agentSystem1);
            agentSystem1.close();
        }
        if (agentSystem2 != null) {
            leaveCluster(agentSystem2);
            agentSystem2.close();
        }
    }

    @Test
    public void shouldRedeployOnOtherInstanceAfterFailure() throws InterruptedException {
        agentSystem1.run(TestAgentVerticle::new).blockingGet();
        agentSystem2.run(TestAgentVerticle::new).blockingGet();

        final Map<String, String> agents = clusterManager.getSyncMap(VertxSupervisorAgent.MAP_NODES);
        final List<VertxAgent> runningAgents = findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 1);
        assertEquals("Should be both instances up",2, runningAgents.size());

        //shutdown one instance
        leaveCluster(agentSystem2);
        //assertEquals("Should be only one instance now", 1, runningAgents.size());
        Thread.sleep(2000);
        //wait for redeploy to happen
        assertEquals("Should be 2 instances after redeployment",2, runningAgents.size());
    }

    @Test
    public void shouldAutoScaleDynamically() throws InterruptedException {
        final AtomicInteger instances = new AtomicInteger(1);
        agentSystem1.run(() -> new AutoScalableAgent(instances), Duration.ofSeconds(3)).blockingGet();

        final Map<String, String> agents = clusterManager.getSyncMap(VertxSupervisorAgent.MAP_NODES);
        assertEquals("Should be one instance up",
                1, findRunningAgents(agents, AutoScalableAgent.class.getSimpleName(), 0).size());

        instances.set(2);

        //wait at least 3 seconds
        Thread.sleep(4000);

        assertEquals("Should be 2 instances up",
                2, findRunningAgents(agents, AutoScalableAgent.class.getSimpleName(), 0).size());

    }

    @Test
    public void shouldUpdateToNewVersion() throws InterruptedException {
        final Map<String, String> agents = clusterManager.getSyncMap(VertxSupervisorAgent.MAP_NODES);
        assertEquals("Should be 0 instances up", 0, findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 1).size());

        agentSystem1.run(() -> new TestAgentVerticle(2, 2, 1)).blockingGet();

        final List<VertxAgent> runningAgents = findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 1);
        assertEquals("Should be 1 instance up",1, runningAgents.size());

        agentSystem2.run(() -> new TestAgentVerticle(1, 2, 1)).blockingGet();
        final List<VertxAgent> runningAgents2 = findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 1);
        assertEquals("Should be 1 instance up because we already have deployed one instance",1, runningAgents2.size());

        agentSystem1.run(() -> new TestAgentVerticle(2, 2, 2)).blockingGet();

        //wait for redeploy to happen
        final List<VertxAgent> runningAgentsNewVersion = findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 2);
        assertEquals("Should be 1 new version instance after deployment",1, runningAgentsNewVersion.size());
        final List<VertxAgent> runningAgentsOldVersion = findRunningAgents(agents, TestAgentVerticle.class.getSimpleName(), 1);
        assertEquals("Should be 1 old version instance running",1, runningAgentsOldVersion.size());
    }

    private static VertxOptions vertxOptions() {
        return new VertxOptions()
                .setHAEnabled(true)
                .setHAGroup("reacto-tests")
                .setClusterManager(new HazelcastClusterManager());
    }

    private void leaveCluster(VertxAgentSystem vertxAgentSystem) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        vertxAgentSystem.clusterManager()
                .ifPresent(cm -> cm.leave(event -> countDownLatch.countDown()));

        countDownLatch.await(10, TimeUnit.SECONDS);
    }

    public class TestAgentVerticle extends AgentVerticle<Long> {

        private final int version;

        TestAgentVerticle() {
            super(VertxAgentOptions.from(new DeploymentOptions())
                    .setHA(true)
                    .setMaxInstancesOnNode(2)
                    .setClusterInstances(2));
            this.version = 1;
        }

        public TestAgentVerticle(int clusterInstances, int maxInstances, int version) {
            super(VertxAgentOptions.from(new DeploymentOptions())
                    .setHA(true)
                    .setMaxInstancesOnNode(maxInstances)
                    .setClusterInstances(clusterInstances));
            this.version = version;
        }

        @Override
        public Flowable<Long> run() {
            return Flowable.interval(0, 1, TimeUnit.SECONDS);
        }

        @Override
        public int version() {
            return version;
        }

    }

    public static class AutoScalableAgent extends AgentVerticle<Long> {

        public AutoScalableAgent(AtomicInteger instances) {
            super(VertxAgentOptions.from(new DeploymentOptions())
                    .setHA(true)
                    .setMaxInstancesOnNode(3)
                    .setClusterInstances(instances::get));
        }

        @Override
        public Flowable<Long> run() {
            return Flowable.interval(0, 1, TimeUnit.SECONDS);
        }

        @Override
        public int version() {
            return 0;
        }
    }
}