package net.soundvibe.reacto.vertx.server.handlers;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import net.soundvibe.reacto.discovery.ServiceDiscoveryLifecycle;
import net.soundvibe.reacto.vertx.server.VertxServer;

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
                Flowable.just(controller)
                        .flatMap(ServiceDiscoveryLifecycle::register)
                        .subscribeOn(Schedulers.io())
                        .subscribe(
                                __ -> writeMessage(ctx, "Service discovery was started successfully")
                                , throwable -> writeError(ctx, throwable));
                break;
            }

            case "close": {
                Flowable.just(controller)
                        .flatMap(ServiceDiscoveryLifecycle::unregister)
                        .subscribeOn(Schedulers.io())
                        .subscribe(
                                __ -> writeMessage(ctx, "Service discovery was closed successfully")
                                , throwable -> writeError(ctx, throwable));
                break;
            }
        }
    }

    private void writeMessage(RoutingContext ctx, String message) {
        ctx.response().end(new JsonObject().put("message", message).encode());
    }

    private void writeError(RoutingContext ctx, Throwable error) {
        ctx.response()
                .setStatusCode(VertxServer.INTERNAL_SERVER_ERROR)
                .setStatusMessage(error.getClass().getSimpleName())
                .end(new JsonObject().put("error", error.toString()).encode());
    }
}
