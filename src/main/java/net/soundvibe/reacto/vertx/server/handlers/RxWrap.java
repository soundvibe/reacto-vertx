package net.soundvibe.reacto.vertx.server.handlers;

import io.vertx.core.*;
import rx.*;
import rx.subjects.*;

import java.util.function.Consumer;

/**
 * @author linas on 17.1.31.
 */
public final class RxWrap<T> implements Handler<AsyncResult<T>> {

    private final Subject<T, T> subject;

    private RxWrap() {
        this.subject = ReplaySubject.create();
    }

    public static <T> Observable<T> using(Consumer<RxWrap<T>> consumer) {
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
            subject.onNext(event.result());
            subject.onCompleted();
        }
    }

    private Observable<T> observe() {
        return subject;
    }
}
