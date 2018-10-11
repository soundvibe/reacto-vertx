package net.soundvibe.reacto.vertx.agent;

import com.codahale.metrics.Counter;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import net.soundvibe.reacto.agent.AgentOptions;
import net.soundvibe.reacto.metric.Metrics;
import net.soundvibe.reacto.utils.WebUtils;
import org.slf4j.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;

public final class VertxSupervisorAgent extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(VertxSupervisorAgent.class);

    static final String MAP_NODES = "__reactoNodes__";
    private static final String MAP_AGENTS_OPTIONS = "__reactoAgentOptions__";
    private static final String ADDRESS_REACTO_AGENT_DEPLOYED = "reactoAgentDeployed";
    private static final String LOCK_REACTO_SUPERVISOR = "reactoSupervisorLock_";
    private final VertxAgentSystem vertxAgentSystem;
    private VertxAgentFactory agentFactory;
    private AgentVerticle<?> agent;
    private VertxAgent vertxAgent;
    private final AtomicReference<String> deploymentId = new AtomicReference<>();
    private Map<String,String> nodes;
    private Map<String,String> agentOptions;
    private VertxAgentOptions vertxAgentOptions;
    private Disposable clusterSyncSubscription;
    private Counter restartCounter;
    private AgentOptions.AgentRestartStrategy restartStrategy;

    public VertxSupervisorAgent(VertxAgentSystem vertxAgentSystem, VertxAgentFactory agentFactory) {
        this.vertxAgentSystem = vertxAgentSystem;
        this.agentFactory = agentFactory;
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        initAgent();
        initAgentOptions();
        this.restartCounter = Metrics.REGISTRY.counter(name(getClass(), agent.name(), "restartCount"));
    }

    @Override
    public void start(Future<Void> startFuture) {
        vertx.deployVerticle(
                agent,
                vertxAgentOptions.toDeploymentOptions(),
                deployment -> handleDeployment(deployment, startFuture));
    }

    @Override
    public void stop(Future<Void> stopFuture) {
        log.info("Stopping supervisor. DeploymentID: " + deploymentID());
        if (isClusterSyncRunning()) clusterSyncSubscription.dispose();
        stopFuture.complete();
    }

    private void initAgent() {
        this.agent = agentFactory.create();
        this.agent.assign(this::handleChildError, this::handleChildComplete);
    }

    private void initAgentOptions() {
        final AgentOptions agentOptions = agent.options();
        if (!(agentOptions instanceof VertxAgentOptions)) {
            throw new IllegalArgumentException("agent.deploymentOptions() should be of VertxAgentOptions class but was: " +
                    agentOptions.getClass().getName());
        }
        this.vertxAgentOptions = (VertxAgentOptions) agentOptions;
        this.restartStrategy = agentOptions.getAgentRestartStrategy();
    }

    private void handleDeployment(AsyncResult<String> deployment, Future<Void> future) {
        try {
            if (deployment.succeeded()) {
                deploymentId.set(deployment.result());

                setAgent(vertxAgentSystem.clusterManager()
                        .map(ClusterManager::getNodeID)
                        .orElse(UUID.randomUUID().toString()));

                vertxAgentSystem.clusterManager()
                        .filter(clusterManager -> vertxAgentOptions.isHA())
                        .ifPresent(clusterManager -> {
                            setAgentInfo(clusterManager);
                            clearOldHaEntries(clusterManager);
                            addToHA();
                            listenForClusterChanges(clusterManager);
                        });

                log.info("{} deployed successfully: {}", agent.name(), deployment.result());
                publishDeployedEvent();
                future.complete();
            } else if (deployment.failed()) {
                log.error("Supervisor was unable to deploy agent: " + agent.name(), deployment.cause());
                future.fail(deployment.cause());
            }
        } catch (Throwable e) {
            log.error("Deployment error: ", e);
            future.fail(e);
        }
    }

    private void clearOldHaEntries(ClusterManager clusterManager) {
        if (clusterManager.getNodes().size() == 1) {
            log.info("We are only node in the cluster. Making sure HA map does not contain more nodes");
            clusterManager.getLockWithTimeout("reacto-ha", 10000, handler -> {
                if (handler.succeeded()) {
                    if (clusterManager.getNodes().size() == 1) {
                        if (!nodes.isEmpty()) {
                            nodes.clear();
                        }
                    }
                }
            });
        }
    }

    private VertxAgent toVertxAgent(String nodeId) {
        return new VertxAgent(nodeId, deploymentId.get(), agent.name(), vertxAgentSystem.group, deploymentID(),
                agent.version(), Instant.now());
    }

    private void addToHA() {
        final String nodeJson = nodes.get(vertxAgent.nodeId);
        if (nodeJson == null) {
            nodes.put(vertxAgent.nodeId, toVertxNode().encode());
        } else {
            final VertxNode vertxNode = VertxNode.fromJson(nodeJson);
            if (vertxNode.agents.stream()
                    .noneMatch(ag -> ag.agentDeploymentId.equals(vertxAgent.agentDeploymentId))) {
                vertxNode.agents.add(vertxAgent);
                nodes.put(vertxNode.nodeId, vertxNode.encode());
            }
        }
    }

    private VertxNode toVertxNode() {
        return new VertxNode(vertxAgent.nodeId, WebUtils.getLocalAddress(), vertxAgentSystem.group,
                Collections.singletonList(vertxAgent));
    }

    private void publishDeployedEvent() {
        vertx.eventBus().publish(ADDRESS_REACTO_AGENT_DEPLOYED, vertxAgent.encode());
    }

    private synchronized void syncDeploymentOptions() {
        final String agentJson = agentOptions.get(agent.name());
        if (agentJson == null) {
            agentOptions.put(agent.name(), vertxAgentOptions.toJson().encode());
        } else {
            final VertxAgentOptions latestVertxAgentOptions = VertxAgentOptions.from(agentJson);
            if (!latestVertxAgentOptions.getAgentRestartStrategy().equals(restartStrategy)) {
                restartStrategy = latestVertxAgentOptions.getAgentRestartStrategy();
            }
            vertxAgentOptions = latestVertxAgentOptions;
        }
    }

    private synchronized void setAgentInfo(ClusterManager clusterManager) {
        this.nodes = clusterManager.getSyncMap(MAP_NODES);
        this.agentOptions = clusterManager.getSyncMap(MAP_AGENTS_OPTIONS);
    }

    private synchronized void setAgent(String nodeId) {
        this.vertxAgent = toVertxAgent(nodeId);
    }

    private void listenForClusterChanges(ClusterManager clusterManager) {
        log.info("Starting to listen for cluster changes for agent: {}", agent.name());
        final MessageConsumer<String> consumer = vertx.eventBus().consumer(ADDRESS_REACTO_AGENT_DEPLOYED);
        consumer.exceptionHandler(error -> log.error("Supervisor eventBus consumer error in agent " + agent.name(), error))
                .handler(message -> {
                    final String agentJson = message.body();
                    final VertxAgent vertxAgent = VertxAgent.fromJson(agentJson);
                    log.info("New agent deployed: {}", vertxAgent);
                    checkForExcessiveAgents(clusterManager, vertxAgent);
                });

        clusterManager.nodeListener(new NodeListener() {
            @Override
            public void nodeAdded(String nodeID) {
                log.info("New node added to cluster: {}", nodeID);
            }

            @Override
            public void nodeLeft(String nodeID) {
                nodes.remove(nodeID);
                log.info("Node left the cluster: {}", nodeID);
                checkForMissingAgents(clusterManager);
            }
        });
    }

    private void checkForMissingAgents(ClusterManager clusterManager) {
        if (isClusterSyncRunning()) return;

        clusterSyncSubscription = Flowable.interval(0, 10, TimeUnit.SECONDS)
                .map(i -> findRunningAgents(nodes, agent.name(), agent.version()))
                .doOnNext(runningAgents -> syncDeploymentOptions())
                .takeWhile(runningAgents -> canBeDeployedLocally(runningAgents, clusterManager.getNodeID()))
                .subscribe(
                        runningAgents -> {
                            log.info("There are less nodes [{}] than desired [{}], will try to redeploy agent: {}",
                                    runningAgents.size(), vertxAgentOptions.getDesiredNumberOfInstances(), agent.name());
                            redeployAgentIfNeeded(runningAgents, clusterManager);
                        },
                        error -> log.error("Error when trying to set desired cluster state: ", error),
                        () -> log.info("Cluster agent [{}] is in it's desired state", agent.name())
                );
    }

    private boolean isClusterSyncRunning() {
        return clusterSyncSubscription != null && !clusterSyncSubscription.isDisposed();
    }

    private boolean canBeDeployedLocally(List<VertxAgent> runningAgents, String localNodeId) {
        final long localInstanceCount = runningAgents.stream()
                .filter(ag -> ag.nodeId.equals(localNodeId))
                .count();
        return runningAgents.size() < vertxAgentOptions.getDesiredNumberOfInstances() &&
                localInstanceCount < vertxAgentOptions.getMaxInstancesOnNode();
    }

    private void checkForExcessiveAgents(ClusterManager clusterManager, VertxAgent newAgent) {
        if (isClusterSyncRunning()) return;
        if (!newAgent.name.equals(this.agent.name())) return;

        clusterSyncSubscription = Flowable.interval(0, 10, TimeUnit.SECONDS)
                .map(i -> findRunningAgents(nodes, this.agent.name(), agent.version()))
                .doOnNext(runningAgents -> syncDeploymentOptions())
                .takeWhile(runningAgents -> runningAgents.size() > vertxAgentOptions.getDesiredNumberOfInstances())
                .subscribe(
                        runningAgents -> {
                            log.info("There are more nodes [{}] than desired [{}], undeploying excessive agent {}...",
                                    runningAgents.size(), vertxAgentOptions.getDesiredNumberOfInstances(), this.agent.name());
                            undeployAgentIfNeeded(runningAgents, clusterManager);
                        },
                        error -> log.error("Error when trying to set excessive cluster state: ", error),
                        () -> log.info("Cluster agent [{}] is in it's desired state", this.agent.name())
                );
    }

    private void undeployAgentIfNeeded(List<VertxAgent> runningAgents, ClusterManager clusterManager) {
        runningAgents.stream()
                .collect(groupingBy(vertxAgent -> vertxAgent.nodeId))
                .values().stream()
                .filter(vertxAgents -> vertxAgents.size() > 1)
                .max(comparing(List::size))
                .map(vertxAgents -> vertxAgents.get(vertxAgents.size() - 1))
                .filter(ag -> ag.nodeId.equals(clusterManager.getNodeID()))
                .ifPresent(toUnDeploy -> clusterManager.getLockWithTimeout(LOCK_REACTO_SUPERVISOR + agent.name(), 2000L, handler -> {
                    if (handler.succeeded()) {
                        final Lock lock = handler.result();
                        if (findRunningAgents(nodes, agent.name(), agent.version())
                                .size() > vertxAgentOptions.getDesiredNumberOfInstances()) {
                            vertx.undeploy(toUnDeploy.supervisorDeploymentId, undeploy -> {
                                if (undeploy.succeeded()) {
                                    log.info("Excessive agent undeployed successfully: {}", toUnDeploy);
                                }
                                lock.release();
                            });
                        }
                    } else {
                        log.warn("Unable to acquire lock", handler.cause());
                    }
                }));
    }

    private void redeployAgentIfNeeded(List<VertxAgent> runningAgents, ClusterManager clusterManager) {
        runningAgents.stream()
                .collect(groupingBy(ag -> ag.nodeId))
                .values().stream()
                .min(comparing(List::size))
                .flatMap(vertxAgents -> vertxAgents.stream()
                        .filter(vertxAgent -> vertxAgent.nodeId.equals(clusterManager.getNodeID()))
                        .findAny())
                .ifPresent(toDeploy ->  clusterManager.getLockWithTimeout(LOCK_REACTO_SUPERVISOR + agent.name(), 2000L, locker -> {
                    if (locker.succeeded()) {
                        final Lock lock = locker.result();
                        vertxAgentSystem.run(agentFactory)
                                .doFinally(lock::release)
                                .subscribe(
                                        () -> log.info("New agent was redeployed successfully: {}", agent.name()),
                                        error -> log.error("Unable to redeploy failed agent: " + agent.name(), error));
                    } else {
                        log.warn("Unable to obtain lock", locker.cause());
                    }
                }));
    }

    public static List<VertxAgent> findRunningAgents(Map<String, String> nodes, String agentName, int version) {
        return nodes.values().stream()
                .map(VertxNode::fromJson)
                .flatMap(vertxNode -> vertxNode.agents.stream())
                .filter(a -> a.name.equals(agentName) && a.version == version)
                .collect(Collectors.toList());
    }

    private void handleChildError(Throwable error) {
        log.error("Error in child agent " + agent.name(), error);
        log.info("Restarting agent {}", agent.name());
        log.info("Using [{}'s] restart strategy: {}", agent.name(), restartStrategy.getClass().getSimpleName());
        boolean wasRestarted = restartStrategy.restart(() -> {
            synchronized (this) {
                initAgent();
                restartCounter.inc();
                agent.start();
            }
        });
        if (!wasRestarted) {
            undeploy();
        }
    }

    private void handleChildComplete() {
        log.info("Got child completed: {}", agent.name());
        if (vertxAgentOptions.isUndeployOnComplete()) {
            undeploy();
        }
    }

    private void undeploy() {
        vertx.undeploy(vertxAgent.supervisorDeploymentId, handler -> {
            if (handler.succeeded()) {
                log.info("Undeployed agent and it's supervisor {}", agent.name());
            } else {
                log.error("Unable to undeploy " + agent.name(), handler.cause());
            }
        });
    }
}
