package net.soundvibe.reacto.vertx.discovery;

import io.vertx.core.json.Json;
import io.vertx.core.logging.*;
import io.vertx.servicediscovery.*;
import io.vertx.servicediscovery.Status;
import io.vertx.servicediscovery.types.HttpEndpoint;
import net.soundvibe.reacto.client.events.EventHandlerRegistry;
import net.soundvibe.reacto.discovery.*;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.errors.CannotDiscoverService;
import net.soundvibe.reacto.internal.ObjectId;
import net.soundvibe.reacto.mappers.ServiceRegistryMapper;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.types.json.JsonObject;
import net.soundvibe.reacto.utils.*;
import net.soundvibe.reacto.vertx.server.*;
import rx.Observable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * @author linas on 17.1.9.
 */
public final class VertxServiceRegistry extends AbstractServiceRegistry implements ServiceDiscoveryLifecycle {

    private static final Logger log = LoggerFactory.getLogger(VertxServiceRegistry.class);

    public static final long DEFAULT_HEARTBEAT_IN_MILLIS = TimeUnit.MINUTES.toMillis(1L);

    private final AtomicReference<Record> record = new AtomicReference<>();
    private final ServiceDiscovery serviceDiscovery;
    private final ServiceRecord serviceRecord;
    private final long heartbeatInMillis;

    public VertxServiceRegistry(EventHandlerRegistry eventHandlerRegistry,
                                ServiceDiscovery serviceDiscovery,
                                ServiceRegistryMapper mapper,
                                ServiceRecord serviceRecord) {
        this(eventHandlerRegistry, serviceDiscovery, mapper, serviceRecord, DEFAULT_HEARTBEAT_IN_MILLIS);
    }

    public VertxServiceRegistry(EventHandlerRegistry eventHandlerRegistry,
                                ServiceDiscovery serviceDiscovery,
                                ServiceRegistryMapper mapper,
                                ServiceRecord serviceRecord,
                                long heartbeatInMillis) {
        super(eventHandlerRegistry, mapper);
        Objects.requireNonNull(serviceRecord, "serviceRecord cannot be null");
        Objects.requireNonNull(serviceDiscovery, "serviceDiscovery cannot be null");
        this.serviceRecord = serviceRecord;
        this.serviceDiscovery = serviceDiscovery;
        this.heartbeatInMillis = heartbeatInMillis;
    }

    static {
        Json.mapper.registerModule(JacksonMapper.jsonTypesModule());
        Json.prettyMapper.registerModule(JacksonMapper.jsonTypesModule());
    }

    @Override
    public Observable<Any> register() {
        log.info("Starting to register service into service discovery...");
        return Observable.just(serviceRecord)
                .map(serviceRec -> record.updateAndGet(rec -> rec == null ?
                        createVertxRecord(serviceRec).setRegistration(null) :
                        rec))
                .flatMap(this::publish)
                .doOnNext(this::startHeartBeat)
                .doOnNext(rec -> Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    log.info("Executing shutdown hook...");
                    if (isRegistered()) {
                        unregister().subscribe(
                                r -> log.debug("Service was unregistered successfully"),
                                e -> log.debug("Error when trying to unregister service: " + e)
                        );
                    }
                })))
                .map(rec -> Any.VOID)
                .subscribeOn(Factories.SINGLE_THREAD);
    }

    @Override
    public Observable<Any> unregister() {
        log.info("Unregistering service from service discovery...");
        return Observable.just(record)
                        .flatMap(r -> r.get() == null ?
                                Observable.error(new IllegalStateException("Cannot unregister service because it was not registered before")) :
                                Observable.just(r.get()))
                        .subscribeOn(Factories.SINGLE_THREAD)
                        .observeOn(Factories.SINGLE_THREAD)
                        .flatMap(rec -> removeIf(rec, VertxRecords::areEquals))
                        .map(rec -> record.updateAndGet(r -> r.setRegistration(null)))
                        .takeLast(1)
                        .map(rec -> Any.VOID)
                        .doOnCompleted(() -> serviceDiscovery.release(serviceDiscovery.getReference(record.get())))
                        .doOnCompleted(serviceDiscovery::close)
                        .doOnCompleted(() -> log.info("Service discovery closed successfully"))
                ;
    }

    @Override
    protected Observable<List<ServiceRecord>> findRecordsOf(Command command) {
        return Observable.create(subscriber ->
                serviceDiscovery.getRecords(record -> VertxRecords.isUpdatedRecently(record, heartbeatInMillis) &&
                                VertxRecords.hasCommand(command.name, command.eventType(), record),
                        false,
                        asyncClients -> {
                            if (asyncClients.succeeded() && !subscriber.isUnsubscribed()) {
                                final List<Record> records = asyncClients.result();
                                if (!records.isEmpty()) {
                                    final Instant now = Instant.now();
                                    records.sort(comparing(rec -> rec.getMetadata().getInstant(VertxRecords.LAST_UPDATED, now)));
                                    subscriber.onNext(records.stream()
                                            .map(VertxServiceRegistry::createServiceRecord)
                                            .collect(toList()));
                                }
                                subscriber.onCompleted();
                            }
                            if (asyncClients.failed() && !subscriber.isUnsubscribed()) {
                                subscriber.onError(new CannotDiscoverService("Unable to find: " + command.name + ":" + command.eventType(), asyncClients.cause()));
                            }
                        })
        );
    }

    @Override
    public Observable<Any> unpublish(ServiceRecord serviceRecord) {
        return Observable.just(serviceRecord)
                .map(rec -> createVertxRecord(serviceRecord))
                .flatMap(rec -> removeIf(rec, VertxRecords::areEquals))
                .takeLast(1)
                .doOnNext(any -> log.info("Unpublished record " + serviceRecord))
                .map(rec -> Any.VOID);
    }

    public boolean isRegistered() {
        return record.get() != null;
    }

    public Observable<Record> cleanServices() {
        return removeRecordsWithStatus(Status.DOWN);
    }

    public static ServiceRecord createServiceRecord(Record record) {
        return ServiceRecord.create(
                record.getName(),
                mapStatus(record.getStatus()),
                mapServiceType(record),
                Optional.ofNullable(record.getRegistration()).orElseGet(() -> ObjectId.get().toString()),
                new JsonObject(Optional.ofNullable(record.getLocation()).map(io.vertx.core.json.JsonObject::getMap)
                        .orElseGet(Collections::emptyMap)),
                new JsonObject(Optional.ofNullable(record.getMetadata()).map(io.vertx.core.json.JsonObject::getMap)
                        .orElseGet(Collections::emptyMap)));
    }

    public static net.soundvibe.reacto.discovery.types.Status mapStatus(Status status) {
        switch (status) {
            case UP:
                return net.soundvibe.reacto.discovery.types.Status.UP;
            case DOWN:
                return net.soundvibe.reacto.discovery.types.Status.DOWN;
            case OUT_OF_SERVICE:
                return net.soundvibe.reacto.discovery.types.Status.OUT_OF_SERVICE;
            case UNKNOWN:
                return net.soundvibe.reacto.discovery.types.Status.UNKNOWN;
            default: return net.soundvibe.reacto.discovery.types.Status.UNKNOWN;
        }
    }

    public static ServiceType mapServiceType(Record record) {
        final String type = Optional.ofNullable(record.getType()).orElse("");
        switch (type) {
            case HttpEndpoint.TYPE:
                return Optional.ofNullable(record.getMetadata())
                        .filter(entries -> entries.getBoolean("isHttp2", false))
                        .map(entries -> ServiceType.WEBSOCKET)
                        .orElse(ServiceType.WEBSOCKET);
            default: return ServiceType.WEBSOCKET;
        }
    }

    public static Record createVertxRecord(ServiceRecord serviceRecord) {
        final String host = serviceRecord.location.asString(ServiceRecord.LOCATION_HOST).orElseGet(WebUtils::getLocalAddress);
        final Integer port = serviceRecord.location.asInteger(ServiceRecord.LOCATION_PORT)
                .orElseThrow(() -> new IllegalArgumentException("port is not found in serviceRecord location"));
        return HttpEndpoint.createRecord(
               serviceRecord.name,
               host,
               port,
               serviceRecord.location.asString(ServiceRecord.LOCATION_ROOT).orElse("/"),
               new io.vertx.core.json.JsonObject(serviceRecord.metaData.encode(Json::encode))
       ).setRegistration(serviceRecord.registrationId);
    }

    private void startHeartBeat(Record record) {
        Scheduler.scheduleAtFixedInterval(heartbeatInMillis, () -> {
            if (isRegistered()) {
                publish(record)
                        .subscribe(rec -> log.debug("Heartbeat published record: " + rec),
                                throwable -> log.error("Error while trying to publish the record on heartbeat: " + throwable),
                                () -> log.debug("Heartbeat completed successfully"));
            } else {
                log.info("Skipping heartbeat because service is not registered");
            }
        }, "service-discovery-heartbeat");
    }


    private Observable<Record> removeIf(Record newRecord, BiPredicate<Record, Record> filter) {
        return Observable.create(subscriber ->
                serviceDiscovery.getRecords(
                        existingRecord -> filter.test(existingRecord, newRecord),
                        true,
                        event -> {
                            if (event.succeeded()) {
                                if (event.result().isEmpty() && !subscriber.isUnsubscribed()) {
                                    subscriber.onNext(newRecord);
                                    subscriber.onCompleted();
                                    return;
                                }

                                Observable.from(event.result())
                                        .doOnNext(record -> serviceDiscovery.release(serviceDiscovery.getReference(record)))
                                        .flatMap(record -> Observable.<Record>create(s -> serviceDiscovery.unpublish(record.getRegistration(), deleteEvent -> {
                                            if (deleteEvent.failed() && (!s.isUnsubscribed())) {
                                                s.onError(deleteEvent.cause());
                                            }
                                            if (deleteEvent.succeeded() && (!s.isUnsubscribed())) {
                                                s.onNext(record);
                                                s.onCompleted();
                                            }
                                        })))
                                        .subscribe(record -> log.info("Record was unpublished: " + record.toJson().encodePrettily()),
                                                throwable -> {
                                                    log.error("Error while trying to unpublish the record: " + throwable);
                                                    subscriber.onNext(newRecord);
                                                    subscriber.onCompleted();
                                                },
                                                () -> {
                                                    subscriber.onNext(newRecord);
                                                    subscriber.onCompleted();
                                                });
                            }
                            if (event.failed()) {
                                log.info("No matching records: " + event.cause());
                                subscriber.onError(event.cause());
                            }
                        }));
    }

    public Record getRecord() {
        return record.get();
    }

    public Observable<Record> publish(Record record) {
        return Observable.just(record)
                .flatMap(rec -> removeIf(rec, (existingRecord, newRecord) -> VertxRecords.isDown(
                        existingRecord, heartbeatInMillis)))
                .map(rec -> {
                    rec.getMetadata().put(VertxRecords.LAST_UPDATED, Instant.now());
                    return rec.setStatus(Status.UP);
                })
                .flatMap(rec -> Observable.create(subscriber -> {
                    if (rec.getRegistration() != null) {
                        serviceDiscovery.update(record, recordEvent -> {
                            if (recordEvent.succeeded()) {
                                log.info("Service has been updated successfully: " + recordEvent.result().toJson());
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onNext(recordEvent.result());
                                    subscriber.onCompleted();
                                }
                            }
                            if (recordEvent.failed()) {
                                log.error("Error when trying to updated the service: " + recordEvent.cause(), recordEvent.cause());
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onError(recordEvent.cause());
                                }
                            }
                        });
                    } else {
                        serviceDiscovery.publish(rec, recordEvent -> {
                            if (recordEvent.succeeded()) {
                                log.info("Service has been published successfully: " + recordEvent.result().toJson());
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onNext(recordEvent.result());
                                    subscriber.onCompleted();
                                }
                            }
                            if (recordEvent.failed()) {
                                log.error("Error when trying to publish the service: " + recordEvent.cause(), recordEvent.cause());
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onError(recordEvent.cause());
                                }
                            }
                        });
                    }
                }));
    }

    public Observable<Record> removeRecordsWithStatus(Status status) {
        return Observable.create(subscriber ->
                serviceDiscovery.getRecords(
                        record -> status.equals(record.getStatus()),
                        true,
                        event -> {
                            if (event.succeeded()) {
                                if (event.result().isEmpty() && !subscriber.isUnsubscribed()) {
                                    subscriber.onCompleted();
                                    return;
                                }
                                Observable.from(event.result())
                                        .flatMap(record -> Observable.<Record>create(subscriber1 ->
                                                serviceDiscovery.unpublish(record.getRegistration(), e -> {
                                                    if (e.failed() && (!subscriber1.isUnsubscribed())) {
                                                        subscriber1.onError(e.cause());
                                                        return;
                                                    }
                                                    if (e.succeeded() && (!subscriber1.isUnsubscribed())) {
                                                        subscriber1.onNext(record);
                                                        subscriber1.onCompleted();
                                                    }
                                                })
                                        ))
                                        .subscribe(subscriber);
                            }
                            if (event.failed()) {
                                log.info("No matching records: " + event.cause());
                                subscriber.onError(event.cause());
                            }
                        }));
    }

}
