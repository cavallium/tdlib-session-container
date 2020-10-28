package it.tdlight.tdlibsession.td.middle.direct;

import static it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer.WAIT_DURATION;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.utils.MonoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class AsyncTdMiddleDirect extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleDirect.class);

	protected final ReplayProcessor<Boolean> tdClosed = ReplayProcessor.cacheLastOrDefault(false);
	protected AsyncTdDirectImpl td;
	private String botAddress;
	private String botAlias;
	private Flux<TdApi.Object> updatesFluxCo;

	public AsyncTdMiddleDirect() {
	}

	public static Mono<AsyncTdMiddleDirect> getAndDeployInstance(TdClusterManager clusterManager,
			String botAlias,
			String botAddress) throws InitializationException {
		try {
			var instance = new AsyncTdMiddleDirect();
			var options = clusterManager.newDeploymentOpts().setConfig(new JsonObject()
					.put("botAlias", botAlias)
					.put("botAddress", botAddress));
			return MonoUtils.<String>executeAsFuture(promise -> {
				clusterManager.getVertx().deployVerticle(instance, options, promise);
			}).doOnNext(_v -> {
				logger.trace("Deployed verticle for bot " + botAlias + ", address: " + botAddress);
			}).thenReturn(instance);
		} catch (RuntimeException e) {
			throw new InitializationException(e);
		}
	}

	@Override
	public void start(Promise<Void> startPromise) {
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

		this.td = new AsyncTdDirectImpl(botAlias);

		td.initializeClient().doOnSuccess(v -> {
			updatesFluxCo = Mono.from(tdClosed).filter(closed -> !closed).flatMapMany(_x -> td.getUpdates(WAIT_DURATION, 1000).flatMap(result -> {
				if (result.succeeded()) {
					if (result.result().succeeded()) {
						return Mono.just(result.result().result());
					} else {
						logger.error("Received an errored update",
								ResponseError.newResponseError("incoming update", botAlias, result.result().cause())
						);
						return Mono.<TdApi.Object>empty();
					}
				} else {
					logger.error("Received an errored update", result.cause());
					return Mono.<TdApi.Object>empty();
				}
			})).publish().refCount(1);
			startPromise.complete();
		}).subscribe(success -> {
		}, (ex) -> {
			logger.error("Failure when starting bot " + botAlias + ", address " + botAddress, ex);
			startPromise.fail(new InitializationException("Can't connect tdlib middle client to tdlib middle server!"));
		}, () -> {});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		tdClosed.onNext(true);
		td.destroyClient().onErrorResume(ex -> {
			logger.error("Can't destroy client", ex);
			return Mono.empty();
		}).doOnTerminate(() -> {
			logger.debug("TdMiddle verticle stopped");
		}).subscribe(MonoUtils.toSubscriber(stopPromise));
	}

	@Override
	public Flux<TdApi.Object> getUpdates() {
		return Flux.from(updatesFluxCo);
	}

	@Override
	public <T extends Object> Mono<TdResult<T>> execute(Function requestFunction, boolean executeDirectly) {
		return td.<T>execute(requestFunction, executeDirectly).onErrorMap(error -> {
			return ResponseError.newResponseError(
					requestFunction,
					botAlias,
					error
			);
		});
	}
}
