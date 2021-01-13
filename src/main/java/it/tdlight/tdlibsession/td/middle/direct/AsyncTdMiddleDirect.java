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
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.utils.MonoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;

public class AsyncTdMiddleDirect extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleDirect.class);

	protected AsyncTdDirectImpl td;
	private String botAddress;
	private String botAlias;
	private Empty<Object> closeRequest = Sinks.empty();

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

		startPromise.complete();
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		closeRequest.tryEmitEmpty();
		stopPromise.complete();
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return td
				.receive(new AsyncTdDirectOptions(WAIT_DURATION, 1000))
				.takeUntilOther(closeRequest.asMono())
				.doOnError(ex -> {
					logger.info("TdMiddle verticle error", ex);
				})
				.doOnTerminate(() -> {
					logger.debug("TdMiddle verticle stopped");
				}).flatMap(result -> {
					if (result.succeeded()) {
						return Mono.just(result.result());
					} else {
						logger.error("Received an errored update",
								ResponseError.newResponseError("incoming update", botAlias, result.cause()));
						return Mono.<TdApi.Object>empty();
					}
				});
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
