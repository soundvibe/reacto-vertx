package net.soundvibe.reacto.vertx.agent;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import net.soundvibe.reacto.agent.AgentOptions;
import org.junit.Test;

import static org.junit.Assert.*;

public class VertxAgentOptionsTest {

    @Test
    public void shouldPersistAllFields() {
        final VertxAgentOptions sut = VertxAgentOptions.from(new DeploymentOptions());
        sut.setMaxInstancesOnNode(1)
                .setHA(true)
                .setClusterInstances(5)
                .setAgentRestartStrategy(AgentOptions.NeverRestart.INSTANCE)
                .setOnCompleteRestartStrategy(new AgentOptions.RestartTimes(5));

        final JsonObject json = sut.toJson();

        final VertxAgentOptions actual = VertxAgentOptions.from(json);

        assertEquals(1, actual.getMaxInstancesOnNode());
        assertEquals(5, actual.getClusterInstances());
        assertTrue(actual.isHA());
    }

    @Test
    public void shouldLoadDefaultValues() {
        final VertxAgentOptions sut = VertxAgentOptions.from(new DeploymentOptions());

        assertEquals(4, sut.getMaxInstancesOnNode());
        assertEquals(1, sut.getClusterInstances());
        assertFalse(sut.isHA());
    }
}