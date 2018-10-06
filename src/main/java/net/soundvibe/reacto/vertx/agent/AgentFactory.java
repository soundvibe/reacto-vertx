package net.soundvibe.reacto.vertx.agent;

public interface AgentFactory<T extends Agent<?>> {

    T create();

}
