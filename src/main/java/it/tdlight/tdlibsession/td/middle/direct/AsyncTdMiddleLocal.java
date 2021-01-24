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
import org.warp.commonutils.error.InitializationException;
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
	private final One<AsyncTdMiddle> cli = Sinks.one();
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
				.onErrorMap(InitializationException::new)
				.doOnNext(this.cli::tryEmitValue)
				.doOnError(this.cli::tryEmitError)
				.thenReturn(this);
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return cli.asMono().single().flatMapMany(AsyncTdMiddle::receive);
	}

	@Override
	public <T extends Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {
		return cli.asMono().single().flatMap(c -> c.execute(request, executeDirectly));
	}
}
