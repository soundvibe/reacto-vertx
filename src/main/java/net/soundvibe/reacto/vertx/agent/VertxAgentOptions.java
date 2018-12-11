package net.soundvibe.reacto.vertx.agent;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import net.soundvibe.reacto.agent.AgentOptions;

import java.util.Optional;

public final class VertxAgentOptions extends AgentOptions {

    private static final String KEY_REACTO_CONFIG = "__reactoConfig__";
    private static final String KEY_CLUSTER_INSTANCES = "clusterInstances";
    private static final String KEY_NODE_MAX_INSTANCES = "nodeMaxInstances";
    private final DeploymentOptions deploymentOptions;

    private VertxAgentOptions(DeploymentOptions deploymentOptions) {
        this.deploymentOptions = deploymentOptions;
    }

    public static VertxAgentOptions from(DeploymentOptions deploymentOptions) {
        final VertxAgentOptions vertxAgentOptions = new VertxAgentOptions(deploymentOptions);

        Optional.ofNullable(deploymentOptions.getConfig())
                .map(config -> config.getJsonObject(KEY_REACTO_CONFIG))
                .ifPresent(reactoConfig -> vertxAgentOptions
                        .setClusterInstances(reactoConfig.getInteger(KEY_CLUSTER_INSTANCES, 1))
                        .setMaxInstancesOnNode(reactoConfig.getInteger(KEY_NODE_MAX_INSTANCES, 4))
                );

        return vertxAgentOptions;
    }

    public static VertxAgentOptions from(JsonObject jsonObject) {
        return from(new DeploymentOptions(jsonObject));
    }

    public static VertxAgentOptions from(String jsonString) {
        return from(new DeploymentOptions(new JsonObject(jsonString)));
    }

    @Override
    public boolean isHA() {
        return super.isHA() || deploymentOptions.isHa();
    }

    @Override
    public VertxAgentOptions setHA(boolean value) {
        deploymentOptions.setHa(value);
        super.setHA(value);
        return this;
    }

    public int getDesiredNumberOfInstances() {
        return Math.max(getClusterInstances() * deploymentOptions.getInstances(), 1);
    }

    public JsonObject toJson() {
        return toDeploymentOptions().toJson();
    }

    public DeploymentOptions toDeploymentOptions() {
        final DeploymentOptions options = new DeploymentOptions(deploymentOptions);
        JsonObject config = options.getConfig();
        if (config == null) {
            config = new JsonObject();
        }

        config.put(KEY_REACTO_CONFIG, new JsonObject()
                .put(KEY_CLUSTER_INSTANCES, getClusterInstances())
                .put(KEY_NODE_MAX_INSTANCES, getMaxInstancesOnNode())
        );

        options.setConfig(config);
        return options;
    }


}
