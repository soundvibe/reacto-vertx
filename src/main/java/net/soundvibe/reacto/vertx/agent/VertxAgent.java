package net.soundvibe.reacto.vertx.agent;

import io.vertx.core.json.Json;

import java.util.Objects;

public final class VertxAgent {

    public final String nodeId;
    public final String agentDeploymentId;
    public final String name;
    public final String group;
    public final String supervisorDeploymentId;

    private VertxAgent() {
        this(null, null, null, null, null);
    }

    public VertxAgent(String nodeId, String agentDeploymentId, String name, String group, String supervisorDeploymentId) {
        this.nodeId = nodeId;
        this.agentDeploymentId = agentDeploymentId;
        this.name = name;
        this.group = group;
        this.supervisorDeploymentId = supervisorDeploymentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VertxAgent)) return false;
        final VertxAgent that = (VertxAgent) o;
        return Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(agentDeploymentId, that.agentDeploymentId) &&
                Objects.equals(name, that.name) &&
                Objects.equals(group, that.group) &&
                Objects.equals(supervisorDeploymentId, that.supervisorDeploymentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, agentDeploymentId, name, group, supervisorDeploymentId);
    }

    public static VertxAgent fromJson(String json) {
        return Json.decodeValue(json, VertxAgent.class);
    }

    public String encode() {
        return Json.encode(this);
    }

    public String encodePrettily() {
        return Json.encodePrettily(this);
    }

    @Override
    public String toString() {
        return "VertxAgent{" +
                "nodeId='" + nodeId + '\'' +
                ", agentDeploymentId='" + agentDeploymentId + '\'' +
                ", name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", supervisorDeploymentId='" + supervisorDeploymentId + '\'' +
                '}';
    }
}
