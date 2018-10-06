package net.soundvibe.reacto.vertx.agent;

import org.reactivestreams.Publisher;

public interface Agent<T> {

    default String name() {
        return getClass().getSimpleName();
    }

    Publisher<T> run();

    AgentDeploymentOptions deploymentOptions();

    default void close() {
        //implement if need to close resources
    }
}
