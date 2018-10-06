package net.soundvibe.reacto.vertx.agent;

import com.codahale.metrics.*;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.*;
import net.soundvibe.reacto.metric.Metrics;
import org.slf4j.*;

import java.util.function.Consumer;

public abstract class ReactoAgent<T> extends AbstractVerticle implements Agent<T> {

    private static final Logger log = LoggerFactory.getLogger(ReactoAgent.class);
    private final Counter eventCounter = Metrics.REGISTRY.counter(MetricRegistry.name(getClass(), "eventCount"));
    private final Counter errorCounter = Metrics.REGISTRY.counter(MetricRegistry.name(getClass(), "errorCount"));
    private final Timer timer = Metrics.REGISTRY.timer(MetricRegistry.name(getClass(), "flowDuration"));

    private final String name;
    private final AgentDeploymentOptions agentDeploymentOptions;

    private Consumer<Throwable> onError;
    private Runnable onComplete;
    private Disposable disposable;

    protected ReactoAgent(AgentDeploymentOptions agentDeploymentOptions) {
        this.name = getClass().getSimpleName();
        this.agentDeploymentOptions = agentDeploymentOptions;
    }

    protected ReactoAgent(String name, AgentDeploymentOptions agentDeploymentOptions) {
        this.name = name;
        this.agentDeploymentOptions = agentDeploymentOptions;
    }

    public abstract Flowable<T> run();

    public String name() {
        return name;
    }

    public AgentDeploymentOptions deploymentOptions() {
        return agentDeploymentOptions;
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        context.exceptionHandler(this::handleError);
    }

    @Override
    public void start() {
        log.info("Starting {}", name());
        final Timer.Context timeCtx = timer.time();
        try {
            disposable = run().subscribe(
                    this::handleEvent,
                    error -> {
                        timeCtx.close();
                        handleError(error);
                    },
                    () -> handleComplete(timeCtx)
            );
        } catch (Throwable e) {
            log.error("Flow cannot be constructed for agent: " + name, e);
            timeCtx.close();
            onError.accept(e);
        }
    }

    @Override
    public void stop() {
        if (disposable == null) throw new IllegalStateException("Agent must be started before stopping");
        log.info("Stopping {}: {}", name(), deploymentID());
        disposable.dispose();
        close();
    }

    private void handleEvent(T event) {
        eventCounter.inc();
    }
    private void handleError(Throwable error) {
        errorCounter.inc();
        onError.accept(error);
    }
    private void handleComplete(Timer.Context timerCtx) {
        log.info("{} flow completed", name);
        timerCtx.close();
        onComplete.run();
    }

    void assign(Consumer<Throwable> onError, Runnable onComplete) {
        this.onError = onError;
        this.onComplete = onComplete;
    }
}
