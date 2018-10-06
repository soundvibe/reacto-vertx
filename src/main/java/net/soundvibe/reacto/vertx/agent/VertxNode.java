package net.soundvibe.reacto.vertx.agent;

import io.vertx.core.json.Json;

import java.util.*;

public final class VertxNode {

    public final String nodeId;
    public final String host;
    public final String group;
    public final List<VertxAgent> agents;

    private VertxNode() {
        this(null, null, null, new ArrayList<>());
    }

    public VertxNode(String nodeId, String host, String group, List<VertxAgent> agents) {
        this.nodeId = nodeId;
        this.host = host;
        this.group = group;
        this.agents = agents;
    }

    public String encode() {
        return Json.encode(this);
    }

    public static VertxNode fromJson(String json) {
        return Json.decodeValue(json, VertxNode.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VertxNode)) return false;
        final VertxNode vertxNode = (VertxNode) o;
        return Objects.equals(nodeId, vertxNode.nodeId) &&
                Objects.equals(host, vertxNode.host) &&
                Objects.equals(group, vertxNode.group) &&
                Objects.equals(agents, vertxNode.agents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, host, group, agents);
    }

    @Override
    public String toString() {
        return "VertxNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", group='" + group + '\'' +
                ", agents=" + agents +
                '}';
    }
}
