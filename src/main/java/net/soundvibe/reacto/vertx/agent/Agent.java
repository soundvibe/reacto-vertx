package net.soundvibe.reacto.vertx.agent;

import io.reactivex.Flowable;

public interface Agent<T> {

    String name();

    Flowable<T> run();

    AgentDeploymentOptions deploymentOptions();

    default void close() {
        //implement if need to close resources
    }
}
