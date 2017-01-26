package net.soundvibe.reacto.vertx.server.handlers;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import net.soundvibe.reacto.discovery.ServiceDiscoveryLifecycle;
import net.soundvibe.reacto.vertx.server.VertxServer;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author OZY on 2016.08.28.
 */
public class ServiceDiscoveryHandler implements Handler<RoutingContext> {

    private final ServiceDiscoveryLifecycle controller;

    public ServiceDiscoveryHandler(ServiceDiscoveryLifecycle controller) {
        this.controller = controller;
    }

    @Override
    public void handle(RoutingContext ctx) {
        final String action = ctx.request().getParam("action");
        if (action == null) {
            ctx.response().setStatusCode(404).setStatusMessage("Action not found").end();
            return;
        }

        switch (action) {
            case "start" : {
                Observable.just(controller)
                        .flatMap(ServiceDiscoveryLifecycle::register)
                        .subscribeOn(Schedulers.io())
                        .subscribe(__ -> ctx.response().end(new JsonObject()
                                .put("message", "Service discovery was started successfully")
                                .encode())
                                , throwable -> ctx.response()
                                        .setStatusCode(VertxServer.INTERNAL_SERVER_ERROR)
                                        .setStatusMessage(throwable.getClass().getSimpleName())
                                        .end(throwable.toString()));
                break;
            }

            case "close": {
                Observable.just(controller)
                        .flatMap(ServiceDiscoveryLifecycle::unregister)
                        .subscribeOn(Schedulers.io())
                        .subscribe(__ -> ctx.response().end(new JsonObject()
                                        .put("message", "Service discovery was closed successfully")
                                        .encode())
                                , throwable -> ctx.response()
                                        .setStatusCode(VertxServer.INTERNAL_SERVER_ERROR)
                                        .setStatusMessage(throwable.getClass().getSimpleName())
                                        .end(throwable.toString()));
                break;
            }
        }
    }
}
