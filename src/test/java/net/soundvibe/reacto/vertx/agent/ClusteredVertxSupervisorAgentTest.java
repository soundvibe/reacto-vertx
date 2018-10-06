package net.soundvibe.reacto.vertx.agent;

import io.reactivex.Flowable;
import io.vertx.core.*;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;

import static io.vertx.core.logging.LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME;
import static org.junit.Assert.assertEquals;

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
    public void tearDown() {
        if (agentSystem1 != null) {
            agentSystem1.close();
        }
        if (agentSystem2 != null) {
            agentSystem2.close();
        }
    }

    @Test
    public void shouldRedeployOnOtherInstanceAfterFailure() throws InterruptedException {
        agentSystem1.run(TestAgent::new).blockingAwait();
        agentSystem2.run(TestAgent::new).blockingAwait();

        final Map<String, String> agents = clusterManager.getSyncMap(VertxSupervisorAgent.MAP_NODES);
        final List<VertxAgent> runningAgents = VertxSupervisorAgent.findRunningAgents(agents, TestAgent.class.getSimpleName());
        assertEquals("Should be both instances up",2, runningAgents.size());

        //shutdown one instance
        leaveCluster(agentSystem2);
        //assertEquals("Should be only one instance now", 1, runningAgents.size());
        Thread.sleep(2000);
        //wait for redeploy to happen
        assertEquals("Should be 2 instances after redeployment",2, runningAgents.size());
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

    public class TestAgent extends ReactoAgent<Long> {

        TestAgent() {
            super(VertxAgentDeploymentOptions.from(new DeploymentOptions()
                    .setInstances(1)
                    .setHa(true))
                    .setHA(true)
                    .setClusterInstances(2));
        }

        @Override
        public Flowable<Long> run() {
            return Flowable.interval(0, 1, TimeUnit.SECONDS);
        }

    }
}