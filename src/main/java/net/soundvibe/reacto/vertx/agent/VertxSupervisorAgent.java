package net.soundvibe.reacto.vertx.agent;

import com.codahale.metrics.Counter;
import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.*;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import net.soundvibe.reacto.agent.*;
import net.soundvibe.reacto.metric.Metrics;
import net.soundvibe.reacto.utils.WebUtils;
import org.slf4j.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
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
    private AgentOptions.AgentRestartStrategy onErrorRestartStrategy;
    private AgentOptions.AgentRestartStrategy onCompleteRestartStrategy;
    private static final ConcurrentMap<String, Boolean> idleSupervisors = new ConcurrentHashMap<>();

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
        vertxAgentSystem.clusterManager().ifPresent(this::setAgentInfo);
    }

    @Override
    public void start(Future<Void> startFuture) {
        try {
            final boolean shouldDeploy = vertxAgentSystem.clusterManager()
                    .map(clusterManager -> findRunningAgents(nodes, agent.name(), agent.version()))
                    .map(vertxAgents -> vertxAgents.size() < vertxAgentOptions.getDesiredNumberOfInstances())
                    .orElse(true);

            if (!shouldDeploy) {
                log.warn("There are already desired number of instances running in the cluster, skipping deployment for {}", agent.name());
                final Boolean result = idleSupervisors.putIfAbsent(agent.name(), true);
                if (result == null || !result) {
                    vertxAgentSystem.clusterManager()
                            .ifPresent(this::listenForClusterChanges);
                    startFuture.complete();
                } else {
                    startFuture.fail(new AgentIsInDesiredClusterState(agent.name(), vertxAgentOptions.getDesiredNumberOfInstances()));
                }
            } else {
                vertx.deployVerticle(
                        agent,
                        vertxAgentOptions.toDeploymentOptions(),
                        deployment -> handleDeployment(deployment, startFuture));
            }
        } catch (Throwable e) {
            startFuture.fail(e);
        }
    }

    @Override
    public void stop(Future<Void> stopFuture) {
        log.info("Stopping supervisor. DeploymentID: " + deploymentID());
        if (isClusterSyncRunning()) clusterSyncSubscription.dispose();
        stopFuture.complete();
    }

    public Optional<String> agentDeploymentId() {
        return Optional.ofNullable(deploymentId.get());
    }

    public Optional<AgentVerticle<?>> agentVerticle() {
        return Optional.ofNullable(agent);
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
        this.onErrorRestartStrategy = agentOptions.getAgentRestartStrategy();
        this.onCompleteRestartStrategy = agentOptions.getOnCompleteRestartStrategy();
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

    private VertxAgent toVertxAgent(String nodeId) {
        return new VertxAgent(nodeId, deploymentId.get(), agent.name(), vertxAgentSystem.group, deploymentID(),
                agent.version(), Instant.now());
    }

    private static final Object lock = new Object();

    private void addToHA() {
        synchronized (lock) {
            if (vertxAgent == null || nodes == null) return;
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
    }

    private void removeFromHA(String deploymentId, DeploymentIdType deploymentIdType) {
        synchronized (lock) {
            if (vertxAgent == null || nodes == null) return;
            final String nodeJson = nodes.get(vertxAgent.nodeId);
            if (nodeJson != null) {
                final VertxNode vertxNode = VertxNode.fromJson(nodeJson);
                if (vertxNode.agents.removeIf(a -> deploymentIdType == DeploymentIdType.SUPERVISOR ?
                        a.supervisorDeploymentId.equals(deploymentId) :
                        a.agentDeploymentId.equals(deploymentId))) {
                    nodes.put(vertxAgent.nodeId, vertxNode.encode());
                }
            }
        }
    }

    @SuppressWarnings("RedundantCollectionOperation")
    private void removeNodeFromHA(String nodeId) {
        synchronized (lock) {
            try {
                if (nodes.containsKey(nodeId)) {
                    nodes.remove(nodeId);
                }
            } catch (Throwable e) {
                log.warn("Cannot remove node from HA: ", e);
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
            if (!latestVertxAgentOptions.getAgentRestartStrategy().equals(onErrorRestartStrategy)) {
                onErrorRestartStrategy = latestVertxAgentOptions.getAgentRestartStrategy();
            }
            if (!latestVertxAgentOptions.getOnCompleteRestartStrategy().equals(onCompleteRestartStrategy)) {
                onCompleteRestartStrategy = latestVertxAgentOptions.getOnCompleteRestartStrategy();
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
                log.info("Node left the cluster: {}", nodeID);
                removeNodeFromHA(nodeID);
                checkForMissingAgents(clusterManager);
            }
        });
    }

    void checkForMissingAgents(ClusterManager clusterManager) {
        if (isClusterSyncRunning()) return;

        clusterSyncSubscription = Flowable.interval(0, 1, TimeUnit.MINUTES)
                .map(i -> findRunningAgents(nodes, agent.name(), agent.version()))
                .doOnNext(runningAgents -> syncDeploymentOptions())
                .takeWhile(runningAgents -> canBeDeployedLocally(runningAgents, clusterManager.getNodeID()))
                .subscribe(
                        runningAgents -> {
                            log.info("There are less nodes [{}] than desired [{}], will try to redeploy agent: {}:{}",
                                    runningAgents.size(), vertxAgentOptions.getDesiredNumberOfInstances(), agent.name(), agent.version());
                            deployAgent(clusterManager);
                        },
                        error -> log.error("Error when trying to set desired cluster state: ", error),
                        () -> log.debug("Cluster agent [{}: {}] is in it's desired state", agent.name(), agent.version())
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

        clusterSyncSubscription = Flowable.interval(0, 1, TimeUnit.MINUTES)
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
                        () -> log.debug("Cluster agent [{}] is in it's desired state", this.agent.name())
                );
    }

    private void undeployAgentIfNeeded(List<VertxAgent> runningAgents, ClusterManager clusterManager) {
        runningAgents.stream()
                .collect(groupingBy(vertxAgent -> vertxAgent.nodeId))
                .values().stream()
                .max(comparing(List::size))
                .map(this::chooseAgent)
                .filter(ag -> isThisNode(ag, clusterManager))
                .ifPresent(toUnDeploy -> undeployAgent(clusterManager, toUnDeploy));
    }

    private boolean isThisNode(VertxAgent vertxAgent, ClusterManager clusterManager) {
        return vertxAgent.nodeId.equals(clusterManager.getNodeID());
    }

    private VertxAgent chooseAgent(List<VertxAgent> vertxAgents) {
        return vertxAgents.stream()
                .min(comparingLong(a -> a.updatedOn.toEpochMilli()))
                .orElseThrow(() -> new NoSuchElementException("No agents to choose from!"));
    }

    private void deployAgent(ClusterManager clusterManager) {
        clusterManager.getLockWithTimeout(LOCK_REACTO_SUPERVISOR + agent.name(), 2000L, locker -> {
            if (locker.succeeded()) {
                final Lock lock = locker.result();
                vertxAgentSystem.run(agentFactory)
                        .subscribeOn(Schedulers.io())
                        .observeOn(Schedulers.io())
                        .doFinally(lock::release)
                        .subscribe(
                                id -> {
                                    log.info("New agent was redeployed successfully: {}", agent.name());
                                    if (deploymentId.get() == null) {
                                        final String name = agent.name();
                                        //no need for idle agent, undeploy itself
                                        undeploy(deploymentID(), DeploymentIdType.SUPERVISOR)
                                                .subscribe(
                                                        () -> idleSupervisors.remove(name),
                                                        e -> log.warn("Unable to undeploy not needed idle supervisor: ", e)
                                        );
                                    }
                                },
                                error -> log.error("Unable to redeploy failed agent: " + agent.name(), error),
                                () -> log.info("No need to redeploy new agent: {}", agent.name()));
            } else {
                log.warn("Unable to obtain lock", locker.cause());
            }
        });
    }

    enum  DeploymentIdType {
        SUPERVISOR, AGENT
    }

    private void undeployAgent(ClusterManager clusterManager, VertxAgent toUnDeploy) {
        clusterManager.getLockWithTimeout(LOCK_REACTO_SUPERVISOR + agent.name(), 2000L, handler -> {
            if (handler.succeeded()) {
                final Lock lock = handler.result();
                log.info("Lock acquired successfully: {}", LOCK_REACTO_SUPERVISOR + agent.name());
                final List<VertxAgent> runningAgents = findRunningAgents(nodes, agent.name(), agent.version());
                if (runningAgents.size() > vertxAgentOptions.getDesiredNumberOfInstances()) {
                    log.info("Starting to undeploy excessive agent: {}", agent.name());
                    final long localAgentCount = runningAgents.stream()
                            .filter(ag -> ag.nodeId.equals(clusterManager.getNodeID()))
                            .count();
                    String unDeploymentId = localAgentCount < 2 ?
                            toUnDeploy.agentDeploymentId :
                            toUnDeploy.supervisorDeploymentId;

                    DeploymentIdType deploymentIdType = localAgentCount < 2 ?
                            DeploymentIdType.AGENT :
                            DeploymentIdType.SUPERVISOR;

                    undeploy(unDeploymentId, deploymentIdType)
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.io())
                            .doFinally(lock::release)
                            .subscribe(
                                    () -> log.info("Excessive agent undeployed successfully: {}", toUnDeploy),
                                    e -> log.warn("Error on excessive agent undeployment: ", e)
                            );
                }
            } else {
                log.warn("Unable to acquire lock", handler.cause());
            }
        });
    }

    public static List<VertxAgent> findRunningAgents(Map<String, String> nodes, String agentName, int version) {
        try {
            return nodes.values().stream()
                    .map(VertxNode::fromJson)
                    .flatMap(vertxNode -> vertxNode.agents.stream())
                    .filter(a -> a.name.equals(agentName) && a.version == version)
                    .collect(Collectors.toList());
        } catch (Throwable e) {
            log.warn("No running agents found: ", e);
            return Collections.emptyList();
        }
    }

    private void handleChildError(Throwable error) {
        log.error("Error in child agent " + agent.name(), error);
        log.info("Restarting agent {}", agent.name());
        log.info("Using [{}'s] restart strategy: {}", agent.name(), onErrorRestartStrategy.getClass().getSimpleName());
        boolean wasRestarted = restartAgent(onErrorRestartStrategy);
        if (!wasRestarted) {
            undeploy()
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .subscribe(
                    () -> log.info("Undeployed successfully on child error: ", error),
                    e -> log.warn("Unable to undeploy on child error: ", e)
            );
        }
    }

    private boolean restartAgent(AgentOptions.AgentRestartStrategy restartStrategy) {
        return restartStrategy.restart(() -> {
            synchronized (this) {
                initAgent();
                restartCounter.inc();
                agent.start();
            }
        });
    }

    private void handleChildComplete() {
        log.info("Got child completed: {}", agent.name());
        switch (vertxAgentOptions.getOnCompleteAction()) {
            case undeploy:
                undeploy()
                    .subscribeOn(Schedulers.io())
                    .observeOn(Schedulers.io())
                    .subscribe(
                        () -> log.info("Undeployed successfully on child complete"),
                        e -> log.warn("Unable to undeploy on child complete: ", e)
                );
                break;
            case restart:
                log.info("Using [{}'s] on complete restart strategy: {}", agent.name(), onCompleteRestartStrategy.getClass().getSimpleName());
                restartAgent(onCompleteRestartStrategy);
                break;
        }
    }

    private Completable undeploy(String deploymentId, DeploymentIdType deploymentIdType) {
        return Completable.create(emitter -> vertx.undeploy(deploymentId, handler -> {
            if (handler.succeeded()) {
                log.info("Undeployed {}: {}", deploymentIdType, agent.name());
                removeFromHA(deploymentId, deploymentIdType);
                if (deploymentIdType == DeploymentIdType.SUPERVISOR) {
                    vertxAgentSystem.removeSupervisor(deploymentId);
                }
                emitter.onComplete();
            } else {
                log.error("Unable to undeploy " + agent.name(), handler.cause());
                emitter.onError(handler.cause());
            }
        }));
    }

    private Completable undeploy() {
        if (vertxAgent == null) return Completable.complete();
        return undeploy(vertxAgent.supervisorDeploymentId, DeploymentIdType.SUPERVISOR);
    }
}
