package net.soundvibe.reacto.vertx.server;

import io.vertx.servicediscovery.Record;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 * @author OZY on 2015.11.24.
 */
public final class Factories {

    private Factories() {
        //
    }

    public static final Scheduler SINGLE_THREAD = Schedulers.from(Executors.newSingleThreadExecutor());

    public static final Predicate<Record> ALL_RECORDS = record -> true;

}
