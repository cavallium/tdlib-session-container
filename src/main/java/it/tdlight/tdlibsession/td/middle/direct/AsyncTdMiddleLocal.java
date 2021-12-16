package it.tdlight.tdlibsession.td.middle.direct;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.client.AsyncTdMiddleEventBusClient;
import it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer;
import it.tdlight.utils.MonoUtils;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;

public class AsyncTdMiddleLocal implements AsyncTdMiddle {

	private final TdClusterManager masterClusterManager;
	private final String botAlias;
	private final int botId;
	private final DeploymentOptions deploymentOptions;
	private final Vertx vertx;

	private final AsyncTdMiddleEventBusServer srv;
	private final AtomicReference<AsyncTdMiddle> cli = new AtomicReference<>(null);
	private final AtomicReference<Throwable> startError = new AtomicReference<>(null);
	private final JsonObject implementationDetails;


	public AsyncTdMiddleLocal(TdClusterManager masterClusterManager,
			String botAlias,
			int botId,
			JsonObject implementationDetails) {
		this.masterClusterManager = masterClusterManager;
		this.botAlias = botAlias;
		this.botId = botId;
		this.implementationDetails = implementationDetails;
		this.vertx = masterClusterManager.getVertx();
		this.deploymentOptions = masterClusterManager
				.newDeploymentOpts()
				.setConfig(new JsonObject()
						.put("botId", botId)
						.put("botAlias", botAlias)
						.put("local", true)
						.put("implementationDetails", implementationDetails)
				);
		this.srv = new AsyncTdMiddleEventBusServer();
	}

	public Mono<AsyncTdMiddleLocal> start() {
		return vertx
				.rxDeployVerticle(srv, deploymentOptions).as(MonoUtils::toMono)
				.single()
				.then(Mono.fromSupplier(() -> new AsyncTdMiddleEventBusClient(masterClusterManager)))
				.zipWith(AsyncTdMiddleEventBusClient.retrieveBinlog(vertx, Path.of("binlogs"), botId))
				.flatMap(tuple -> tuple
						.getT1()
						.start(botId, botAlias, true, implementationDetails, tuple.getT2())
						.thenReturn(tuple.getT1()))
				.onErrorMap(IllegalStateException::new)
				.doOnNext(this.cli::set)
				.doOnError(this.startError::set)
				.thenReturn(this);
	}

	@Override
	public Mono<Void> initialize() {
		var startError = this.startError.get();
		if (startError != null) {
			return Mono.error(startError);
		}
		return Mono.fromCallable(cli::get).single().flatMap(AsyncTdMiddle::initialize);
	}

	@Override
	public Flux<TdApi.Object> receive() {
		var startError = this.startError.get();
		if (startError != null) {
			return Flux.error(startError);
		}
		return Mono.fromCallable(cli::get).single().flatMapMany(AsyncTdMiddle::receive);
	}

	@Override
	public <T extends Object> Mono<TdResult<T>> execute(Function<T> request, Duration timeout, boolean executeDirectly) {
		var startError = this.startError.get();
		if (startError != null) {
			return Mono.error(startError);
		}
		return Mono.fromCallable(cli::get).single().flatMap(c -> c.execute(request, timeout, executeDirectly));
	}
}
