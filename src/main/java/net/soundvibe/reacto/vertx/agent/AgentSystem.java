package net.soundvibe.reacto.vertx.agent;

import io.reactivex.Completable;

import java.io.Closeable;

public interface AgentSystem<T extends AgentFactory<?>> extends Closeable {

    Completable run(T agentFactory);
}
