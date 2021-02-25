package it.tdlight.tdlibsession.td.middle.direct;

import static it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer.WAIT_DURATION;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.direct.TelegramClientFactory;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.utils.MonoUtils;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;

public class AsyncTdMiddleDirect extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleDirect.class);

	private final TelegramClientFactory clientFactory;

	protected AsyncTdDirectImpl td;
	private String botAddress;
	private String botAlias;
	private final Empty<Object> closeRequest = Sinks.empty();

	public AsyncTdMiddleDirect() {
		this.clientFactory = new TelegramClientFactory();
	}

	public static Mono<AsyncTdMiddle> getAndDeployInstance(TdClusterManager clusterManager,
			String botAlias,
			String botAddress,
			JsonObject implementationDetails) {
			var instance = new AsyncTdMiddleDirect();
			var options = clusterManager.newDeploymentOpts().setConfig(new JsonObject()
					.put("botAlias", botAlias)
					.put("botAddress", botAddress)
					.put("implementationDetails", implementationDetails));
		return clusterManager.getVertx()
				.rxDeployVerticle(instance, options)
				.as(MonoUtils::toMono)
				.doOnNext(_v -> logger.trace("Deployed verticle for bot " + botAlias + ", address: " + botAddress))
				.thenReturn(instance);
	}

	@Override
	public Completable rxStart() {
		var botAddress = config().getString("botAddress");
		if (botAddress == null || botAddress.isEmpty()) {
			throw new IllegalArgumentException("botAddress is not set!");
		}
		this.botAddress = botAddress;
		var botAlias = config().getString("botAlias");
		if (botAlias == null || botAlias.isEmpty()) {
			throw new IllegalArgumentException("botAlias is not set!");
		}
		this.botAlias = botAlias;
		var implementationDetails = config().getJsonObject("implementationDetails");
		if (implementationDetails == null) {
			throw new IllegalArgumentException("implementationDetails is not set!");
		}

		this.td = new AsyncTdDirectImpl(clientFactory, implementationDetails, botAlias);

		return Completable.complete();
	}

	@Override
	public Completable rxStop() {
		closeRequest.tryEmitEmpty();
		return Completable.complete();
	}

	@Override
	public Mono<Void> initialize() {
		return td
				.initialize();
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return td
				.receive(new AsyncTdDirectOptions(WAIT_DURATION, 100))
				.takeUntilOther(closeRequest.asMono())
				.doOnNext(s -> logger.trace("Received update from tdlib: {}", s.getClass().getSimpleName()))
				.doOnError(ex -> logger.info("TdMiddle verticle error", ex))
				.doOnTerminate(() -> logger.debug("TdMiddle verticle stopped"))
				.subscribeOn(Schedulers.parallel());
	}

	@Override
	public <T extends Object> Mono<TdResult<T>> execute(Function requestFunction, boolean executeDirectly) {
		return td
				.<T>execute(requestFunction, executeDirectly)
				.onErrorMap(error -> ResponseError.newResponseError(requestFunction, botAlias, error));
	}
}
