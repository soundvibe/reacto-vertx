package net.soundvibe.reacto.vertx.agent;

import com.codahale.metrics.*;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.*;
import net.soundvibe.reacto.agent.*;
import net.soundvibe.reacto.metric.Metrics;
import org.slf4j.*;

import java.util.function.Consumer;

public abstract class AgentVerticle<T> extends AbstractVerticle implements Agent<T> {

    private static final Logger log = LoggerFactory.getLogger(AgentVerticle.class);
    private final Meter errorMeter = Metrics.REGISTRY.meter(MetricRegistry.name(getClass(), "errorMeter"));
    private final Timer timer = Metrics.REGISTRY.timer(MetricRegistry.name(getClass(), "flowDuration"));
    private final Meter eventMeter = Metrics.REGISTRY.meter(MetricRegistry.name(getClass(), "eventMeter"));

    private final String name;
    private final AgentOptions agentOptions;

    private Consumer<Throwable> onError;
    private Runnable onComplete;
    private Disposable disposable;

    protected AgentVerticle(AgentOptions agentOptions) {
        this.name = getClass().getSimpleName();
        this.agentOptions = agentOptions;
    }

    protected AgentVerticle(String name, AgentOptions agentOptions) {
        this.name = name;
        this.agentOptions = agentOptions;
    }

    @Override
    public abstract Flowable<T> run();

    @Override
    public String name() {
        return name;
    }

    @Override
    public AgentOptions options() {
        return agentOptions;
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
        eventMeter.mark();
    }
    private void handleError(Throwable error) {
        errorMeter.mark();
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
