package net.soundvibe.reacto.vertx.server.handlers;

import io.reactivex.Flowable;
import io.reactivex.processors.*;
import io.vertx.core.*;

import java.util.function.Consumer;

/**
 * @author linas on 17.1.31.
 */
public final class RxWrap<T> implements Handler<AsyncResult<T>> {

    private final FlowableProcessor<T> subject;

    private RxWrap() {
        this.subject = ReplayProcessor.create();
    }

    public static <T> Flowable<T> using(Consumer<RxWrap<T>> consumer) {
        final RxWrap<T> wrapper = new RxWrap<>();
        consumer.accept(wrapper);
        return wrapper.observe();
    }

    @Override
    public void handle(AsyncResult<T> event) {
        if (event.failed()) {
            subject.onError(event.cause());
            return;
        }

        if (event.succeeded()) {
            if (event.result() != null) {
                subject.onNext(event.result());
            }
            subject.onComplete();
        }
    }

    private Flowable<T> observe() {
        return subject;
    }
}
