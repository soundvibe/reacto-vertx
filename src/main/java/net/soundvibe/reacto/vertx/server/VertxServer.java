package net.soundvibe.reacto.vertx.server;

import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.*;
import io.vertx.ext.web.Router;
import net.soundvibe.reacto.discovery.ServiceDiscoveryLifecycle;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.Any;
import net.soundvibe.reacto.vertx.server.handlers.*;
import rx.Observable;

import java.util.Objects;

import static net.soundvibe.reacto.utils.WebUtils.*;

/**
 * @author OZY on 2015.11.23.
 */
public class VertxServer implements Server<HttpServer> {

    public static final int INTERNAL_SERVER_ERROR = 500;

    private static final Logger log = LoggerFactory.getLogger(VertxServer.class);

    public static final String HYSTRIX_STREAM_PATH = "hystrix.stream";
    public static final String REACTO_STREAM_PATH = "reacto.stream";

    private final ServiceOptions serviceOptions;
    private final CommandRegistry commands;
    private final HttpServer httpServer;
    private final Router router;
    private final ServiceDiscoveryLifecycle discoveryLifecycle;

    public VertxServer(
            ServiceOptions serviceOptions,
            Router router,
            HttpServer httpServer,
            CommandRegistry commands,
            ServiceDiscoveryLifecycle discoveryLifecycle) {
        Objects.requireNonNull(serviceOptions, "serviceOptions cannot be null");
        Objects.requireNonNull(router, "Router cannot be null");
        Objects.requireNonNull(httpServer, "HttpServer cannot be null");
        Objects.requireNonNull(commands, "CommandRegistry cannot be null");
        Objects.requireNonNull(discoveryLifecycle, "discoveryLifecycle cannot be null");
        this.serviceOptions = serviceOptions;
        this.router = router;
        this.httpServer = httpServer;
        this.commands = commands;
        this.discoveryLifecycle = discoveryLifecycle;
    }

    @Override
    public Observable<HttpServer> start() {
        return Observable.just(httpServer)
                .doOnNext(server -> setupRoutes())
                .flatMap(server -> RxWrap.<HttpServer>using(httpServer::listen))
                .flatMap(server -> discoveryLifecycle.register()
                        .map(__ -> server));
    }

    @Override
    public Observable<Any> stop() {
        return RxWrap.<Void>using(httpServer::close)
                .flatMap(__ -> discoveryLifecycle.unregister());
    }

    private void setupRoutes() {
        if (commands.stream().findAny().isPresent()) {
            httpServer.websocketHandler(new WebSocketCommandHandler(new CommandProcessor(commands), root()));
            router.route(root() + HYSTRIX_STREAM_PATH)
                    .handler(new SSEHandler(HystrixEventStreamHandler::handle));
            router.route(root() + "service-discovery/:action")
                    .produces("application/json")
                    .handler(new ServiceDiscoveryHandler(discoveryLifecycle));
        }
        httpServer.requestHandler(router::accept);
    }

    private String root() {
        return includeEndDelimiter(includeStartDelimiter(serviceOptions.root));
    }

}
