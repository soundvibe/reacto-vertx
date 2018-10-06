package net.soundvibe.reacto.vertx.agent;

public abstract class AgentDeploymentOptions {

    private int clusterInstances = 1;
    private boolean isHA = false;
    private String clusterGroup;

    public boolean isHA() {
        return isHA;
    }

    public AgentDeploymentOptions setHA(boolean value) {
        this.isHA = value;
        return this;
    }

    public int getClusterInstances() {
        return clusterInstances;
    }

    public AgentDeploymentOptions setClusterInstances(int clusterInstances) {
        if (clusterInstances < 1) throw new IllegalArgumentException("Cluster instances cannot be less than 1 but was " + clusterInstances);
        this.clusterInstances = clusterInstances;
        return this;
    }
}
