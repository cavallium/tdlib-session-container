package it.tdlight.tdlibsession.td.middle.client;

import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.EventBusFlux;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.utils.MonoUtils;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;

public class AsyncTdMiddleEventBusClient extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusClient.class );

	public static final boolean OUTPUT_REQUESTS = false;
	public static final byte[] EMPTY = new byte[0];

	private final TdClusterManager cluster;
	private final DeliveryOptions deliveryOptions;
	private final DeliveryOptions deliveryOptionsWithTimeout;

	private final Empty<Void> tdCloseRequested = Sinks.empty();
	private final Empty<Void> tdClosed = Sinks.empty();
	private final One<Error> tdCrashed = Sinks.one();

	private String botAddress;
	private String botAlias;
	private boolean local;
	private long initTime;

	public AsyncTdMiddleEventBusClient(TdClusterManager clusterManager) {
		cluster = clusterManager;
		this.deliveryOptions = cluster.newDeliveryOpts().setLocalOnly(local);
		this.deliveryOptionsWithTimeout = cluster.newDeliveryOpts().setLocalOnly(local).setSendTimeout(30000);
	}

	public static Mono<AsyncTdMiddle> getAndDeployInstance(TdClusterManager clusterManager,
			String botAlias,
			String botAddress,
			boolean local) {
		var instance = new AsyncTdMiddleEventBusClient(clusterManager);
		var options = clusterManager
				.newDeploymentOpts()
				.setConfig(new JsonObject().put("botAddress", botAddress).put("botAlias", botAlias).put("local", local));
		return clusterManager
				.getVertx()
				.rxDeployVerticle(instance, options)
				.as(MonoUtils::toMono)
				.doOnSuccess(s -> logger.trace("Deployed verticle for bot address: " + botAddress))
				.thenReturn(instance);
	}

	@Override
	public Completable rxStart() {
		return Mono
				.fromCallable(() -> {
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
					var local = config().getBoolean("local");
					if (local == null) {
						throw new IllegalArgumentException("local is not set!");
					}
					this.local = local;
					this.initTime = System.currentTimeMillis();
					return null;
				})
				.then(Mono.create(future -> {
					logger.debug("Waiting for " + botAddress + ".readyToStart");
					var readyToStartConsumer = cluster.getEventBus().<byte[]>consumer(botAddress + ".readyToStart");
					readyToStartConsumer.handler((Message<byte[]> pingMsg) -> {
						logger.debug("Received ping reply (succeeded)");
						readyToStartConsumer
								.rxUnregister()
								.as(MonoUtils::toMono)
								.doOnError(ex -> {
									logger.error("Failed to unregister readyToStartConsumer", ex);
								})
								.then(Mono.fromCallable(() -> {
									pingMsg.reply(new byte[0]);
									logger.debug("Requesting " + botAddress + ".start");
									return null;
								}))
								.then(cluster
										.getEventBus()
										.rxRequest(botAddress + ".start", EMPTY, deliveryOptionsWithTimeout)
										.as(MonoUtils::toMono)
										.doOnError(ex -> logger.error("Failed to request bot start", ex)))
								.doOnNext(msg -> logger.debug("Requesting " + botAddress + ".isWorking"))
								.then(cluster.getEventBus()
										.rxRequest(botAddress + ".isWorking", EMPTY, deliveryOptionsWithTimeout)
										.as(MonoUtils::toMono)
										.doOnError(ex -> logger.error("Failed to request isWorking", ex)))
								.subscribeOn(Schedulers.single())
								.subscribe(v -> {}, future::error, future::success);
					});

					readyToStartConsumer
							.rxCompletionHandler()
							.as(MonoUtils::toMono)
							.doOnSuccess(s -> logger.debug("Requesting tdlib.remoteclient.clients.deploy.existing"))
							.then(Mono.fromCallable(() -> cluster.getEventBus().publish("tdlib.remoteclient.clients.deploy", botAddress, deliveryOptions)))
							.subscribe(v -> {}, future::error);
					})
				)
				.onErrorMap(ex -> {
					logger.error("Failure when starting bot " + botAddress, ex);
					return new InitializationException("Can't connect tdlib middle client to tdlib middle server!");
				})
				.as(MonoUtils::toCompletable);
	}

	@Override
	public Completable rxStop() {
		return Mono
				.fromRunnable(() -> logger.debug("Stopping AsyncTdMiddle client verticle..."))
				.then(Mono
						.firstWithSignal(
								tdCloseRequested.asMono(),
								tdClosed.asMono(),
								Mono.firstWithSignal(
										tdCrashed.asMono(),
										Mono
												.fromRunnable(() -> logger.warn("Verticle is being stopped before closing TDLib with Close()! Sending Close() before stopping..."))
												.then(this.execute(new TdApi.Close(), false))
												.then()
								)
								.doOnTerminate(() -> {
									logger.debug("Close() sent to td");
									markCloseRequested();
								})
								.then(tdClosed.asMono())
						)
				)
				.doOnError(ex -> logger.debug("Failed to stop AsyncTdMiddle client verticle"))
				.doOnSuccess(s -> logger.debug("Stopped AsyncTdMiddle client verticle"))
				.as(MonoUtils::toCompletable);
	}

	@Override
	public Flux<TdApi.Object> receive() {
		var fluxCodec = new TdResultListMessageCodec();
		return Flux
				.firstWithSignal(
						tdCloseRequested.asMono().flux().cast(TdApi.Object.class),
						tdClosed.asMono().flux().cast(TdApi.Object.class),
						EventBusFlux
								.<TdResultList>connect(cluster.getEventBus(),
										botAddress + ".updates",
										deliveryOptions,
										fluxCodec,
										Duration.ofMillis(deliveryOptionsWithTimeout.getSendTimeout())
								)
								.filter(Objects::nonNull)
								.flatMap(block -> Flux.fromIterable(block.getValues()))
								.filter(Objects::nonNull)
								.onErrorResume(error -> {
									TdApi.Error theError;
									if (error instanceof ConnectException) {
										theError = new Error(444, "CONNECTION_KILLED");
									} else if (error.getMessage().contains("Timed out")) {
										theError = new Error(444, "CONNECTION_KILLED");
									} else {
										theError = new Error(406, "INVALID_UPDATE");
										logger.error("Bot updates request failed! Marking as closed.", error);
									}
									tdCrashed.tryEmitValue(theError);
									return Flux.just(TdResult.failed(theError));
								}).flatMap(item -> Mono.fromCallable(item::orElseThrow))
								.filter(Objects::nonNull)
								.doOnNext(item -> {
									if (OUTPUT_REQUESTS) {
										System.out.println(" <- " + item.toString()
												.replace("\n", " ")
												.replace("\t", "")
												.replace("  ", "")
												.replace(" = ", "=")
										);
									}
									if (item.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
										var state = (UpdateAuthorizationState) item;
										if (state.authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR) {
											// Send tdClosed early to avoid errors
											logger.debug("Received AuthorizationStateClosed from td. Marking td as closed");
											markCloseRequested();
											markClosed();
										}
									}
								}).doFinally(s -> {
							if (s == SignalType.ON_ERROR) {
								// Send tdClosed early to avoid errors
								logger.debug("Updates flux terminated with an error signal. Marking td as closed");
								markCloseRequested();
								markClosed();
							}
						})
				);
	}

	private void markCloseRequested() {
		if (tdCloseRequested.tryEmitEmpty().isFailure()) {
			logger.error("Failed to set tdCloseRequested");
		}
	}

	private void markClosed() {
		if (tdClosed.tryEmitEmpty().isFailure()) {
			logger.error("Failed to set tdClosed");
		}
		if (tdCrashed.tryEmitEmpty().isFailure()) {
			logger.debug("TDLib already crashed");
		}
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {
		var req = new ExecuteObject(executeDirectly, request);
		return Mono
				.fromRunnable(() -> {
					if (OUTPUT_REQUESTS) {
						System.out.println(" -> " + request.toString()
								.replace("\n", " ")
								.replace("\t", "")
								.replace("  ", "")
								.replace(" = ", "="));
					}
				})
				.then(Mono.firstWithSignal(
						tdCloseRequested.asMono().flatMap(t -> Mono.empty()),
						tdClosed.asMono().flatMap(t -> Mono.empty()),
						cluster.getEventBus()
								.<TdResultMessage>rxRequest(botAddress + ".execute", req, deliveryOptions)
								.as(MonoUtils::toMono)
								.onErrorMap(ex -> ResponseError.newResponseError(request, botAlias, ex))
								.<TdResult<T>>flatMap(resp -> resp.body() == null ? Mono.<TdResult<T>>error(new NullPointerException("Response is empty")) : Mono.just(resp.body().toTdResult()))
								.switchIfEmpty(Mono.defer(() -> Mono.just(TdResult.<T>failed(new TdApi.Error(500, "Client is closed or response is empty")))))
								.doOnNext(response -> {
									if (OUTPUT_REQUESTS) {
										System.out.println(" <- " + response.toString()
												.replace("\n", " ")
												.replace("\t", "")
												.replace("  ", "")
												.replace(" = ", "="));
									}
								})
				));
	}
}
