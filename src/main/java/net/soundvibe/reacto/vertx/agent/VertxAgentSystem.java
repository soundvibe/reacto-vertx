package net.soundvibe.reacto.vertx.agent;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.*;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.Json;
import io.vertx.core.spi.cluster.ClusterManager;
import net.soundvibe.reacto.agent.*;
import org.slf4j.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.*;

public final class VertxAgentSystem implements AgentSystem<VertxAgentFactory>, io.vertx.core.Closeable {

    private static final Logger log = LoggerFactory.getLogger(VertxAgentSystem.class);
    private static final String GROUP_DEFAULT = VertxOptions.DEFAULT_HA_GROUP;

    private final ConcurrentMap<String, VertxSupervisorAgent> deployedSupervisors = new ConcurrentHashMap<>();

    public final Vertx vertx;
    public final String group;
    private final AtomicReference<Disposable> syncRef = new AtomicReference<>();

    private VertxAgentSystem(Vertx vertx, String group) {
        this.vertx = vertx;
        this.group = group;
    }

    public static VertxAgentSystem of(Vertx vertx) {
        return of(vertx, GROUP_DEFAULT);
    }

    public static VertxAgentSystem ofClustered(VertxOptions vertxOptions) {
        return ofClustered(vertxOptions, Duration.ofSeconds(30));
    }

    public static VertxAgentSystem of(VertxOptions vertxOptions) {
        return vertxOptions.getClusterManager() != null ?
                ofClustered(vertxOptions) :
                of(Vertx.vertx(vertxOptions), vertxOptions.getHAGroup());
    }

    public static VertxAgentSystem ofClustered(VertxOptions vertxOptions, Duration waitTimeOut) {
        if (vertxOptions.getClusterManager() == null)
            throw new IllegalArgumentException("ClusterManager must be set for VertxOptions");
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<AsyncResult<Vertx>> vertxRef = new AtomicReference<>();
        Vertx.clusteredVertx(vertxOptions, handler -> {
            vertxRef.set(handler);
            countDownLatch.countDown();
        });
        try {
            countDownLatch.await(waitTimeOut.toMillis(), TimeUnit.MILLISECONDS);
            final AsyncResult<Vertx> result = vertxRef.get();
            if (result == null) throw new RuntimeException("Unable to obtain clustered vertx");
            if (result.failed()) throw new RuntimeException(result.cause());
            return of(result.result(), vertxOptions.getHAGroup());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static VertxAgentSystem of(Vertx vertx, String group) {
        return new VertxAgentSystem(vertx, group);
    }

    static {
        Json.mapper.registerModule(new JavaTimeModule());
        Json.prettyMapper.registerModule(new JavaTimeModule());
        Json.mapper.registerModule(new Jdk8Module());
        Json.prettyMapper.registerModule(new Jdk8Module());
    }

    @Override
    public Maybe<String> run(VertxAgentFactory agentFactory) {
        return Maybe.create(emitter -> {
            final String uuid = UUID.randomUUID().toString();
            final VertxSupervisorAgent supervisorAgent = new VertxSupervisorAgent(this, agentFactory);
            vertx.deployVerticle(supervisorAgent,
                    new DeploymentOptions()
                            .setWorker(true)
                            .setInstances(1)
                            .setHa(true),
                    handler -> {
                        if (handler.succeeded()) {
                            log.info("Supervisor deployed for {}. ID: {}", uuid, handler.result());
                            deployedSupervisors.put(handler.result(), supervisorAgent);
                            initSyncIfNeeded();
                            emitter.onSuccess(handler.result());
                        } else if (handler.failed()) {
                            final Throwable cause = handler.cause();
                            if (cause instanceof AgentIsInDesiredClusterState) {
                                log.warn("Agent is is desired cluster state, init sync if needed...");
                                initSyncIfNeeded();
                                emitter.onComplete();
                            } else {
                                log.error("Unable to deploy agent: " + uuid, cause);
                                emitter.onError(cause);
                            }
                        }
                    });
        });
    }

    public int deployedSupervisorsCount() {
        return deployedSupervisors.size();
    }

    public List<VertxSupervisorAgent> deployedSupervisors() {
        return new ArrayList<>(deployedSupervisors.values());
    }

    public List<AgentVerticle<?>> deployedAgents() {
        return deployedSupervisors.values().stream()
                .flatMap(vertxSupervisorAgent -> vertxSupervisorAgent.agentDeploymentId()
                        .map(t -> vertxSupervisorAgent.agentVerticle().map(Stream::of).orElse(Stream.empty()))
                        .orElse(Stream.empty()))
                .collect(Collectors.toList());
    }

    public Optional<ClusterManager> clusterManager() {
        if (!vertx.isClustered()) return Optional.empty();
        if (vertx instanceof VertxInternal) {
            VertxInternal vertxInternal = (VertxInternal) vertx;
            return Optional.ofNullable(vertxInternal.getClusterManager());
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        closeSync();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        vertx.close(handler -> countDownLatch.countDown());
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Unable to close agent system gracefully: ", e);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        closeSync();
        vertx.close(completionHandler);
    }

    private synchronized void initSyncIfNeeded() {
        if (!clusterManager().isPresent()) return;
        final ClusterManager clusterManager = clusterManager().get();
        final Disposable disposable = syncRef.get();
        if (disposable == null || disposable.isDisposed()) {
            final Disposable subscription = Flowable.interval(1, 1, TimeUnit.MINUTES, Schedulers.io())
                    .flatMapIterable(i -> deployedSupervisors())
                    .subscribe(
                            vertxSupervisorAgent -> {
                                try {
                                    vertxSupervisorAgent.checkForMissingAgents(clusterManager);
                                } catch (Throwable e) {
                                    log.warn("CheckForMissingAgents threw an error: ", e);
                                }
                            },
                            e -> log.error("Agent System sync failed:", e),
                            () -> log.info("Agent System sync completed")
                    );
            syncRef.set(subscription);
        }
    }

    void removeSupervisor(String deploymentId) {
        deployedSupervisors.remove(deploymentId);
    }

    private synchronized void closeSync() {
        final Disposable disposable = syncRef.get();
        if (disposable != null) {
            disposable.dispose();
        }
    }
}
