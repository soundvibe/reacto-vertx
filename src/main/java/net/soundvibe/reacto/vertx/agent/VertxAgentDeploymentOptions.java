package net.soundvibe.reacto.vertx.agent;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

public final class VertxAgentDeploymentOptions extends AgentDeploymentOptions {

    private static final String KEY_REACTO_CONFIG = "__reactoConfig__";
    private static final String KEY_CLUSTER_INSTANCES = "clusterInstances";
    private final DeploymentOptions deploymentOptions;

    private VertxAgentDeploymentOptions(DeploymentOptions deploymentOptions) {
        this.deploymentOptions = deploymentOptions;
    }

    public static VertxAgentDeploymentOptions from(DeploymentOptions deploymentOptions) {
        final VertxAgentDeploymentOptions vertxAgentDeploymentOptions = new VertxAgentDeploymentOptions(deploymentOptions);

        Optional.ofNullable(deploymentOptions.getConfig())
                .map(config -> config.getJsonObject(KEY_REACTO_CONFIG))
                .ifPresent(reactoConfig -> vertxAgentDeploymentOptions
                        .setClusterInstances(reactoConfig.getInteger(KEY_CLUSTER_INSTANCES, 1)));

        return vertxAgentDeploymentOptions;
    }

    public static VertxAgentDeploymentOptions from(JsonObject jsonObject) {
        return from(new DeploymentOptions(jsonObject));
    }

    public static VertxAgentDeploymentOptions from(String jsonString) {
        return from(new DeploymentOptions(new JsonObject(jsonString)));
    }

    @Override
    public boolean isHA() {
        return super.isHA() || deploymentOptions.isHa();
    }

    @Override
    public VertxAgentDeploymentOptions setHA(boolean value) {
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
        );

        options.setConfig(config);
        return options;
    }


}
