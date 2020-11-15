package it.tdlight.tdlibsession.td.middle.client;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import it.tdlight.common.ConstructorDetector;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdExecuteObjectMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdOptListMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdOptionalList;
import it.tdlight.tdlibsession.td.middle.TdResultMessageCodec;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class AsyncTdMiddleEventBusClient extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusClient.class );

	public static final boolean OUTPUT_REQUESTS = false;
	public static final byte[] EMPTY = new byte[0];

	private final ReplayProcessor<Boolean> tdClosed = ReplayProcessor.cacheLastOrDefault(false);
	private final DeliveryOptions deliveryOptions;
	private final DeliveryOptions deliveryOptionsWithTimeout;

	private ReplayProcessor<Flux<TdApi.Object>> incomingUpdatesCo = ReplayProcessor.cacheLast();

	private TdClusterManager cluster;

	private String botAddress;
	private String botAlias;
	private boolean local;
	private long initTime;
	private MessageConsumer<byte[]> readyToStartConsumer;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public AsyncTdMiddleEventBusClient(TdClusterManager clusterManager) {
		cluster = clusterManager;
		if (cluster.registerDefaultCodec(TdOptionalList.class, new TdOptListMessageCodec())) {
			cluster.registerDefaultCodec(ExecuteObject.class, new TdExecuteObjectMessageCodec());
			cluster.registerDefaultCodec(TdResultMessage.class, new TdResultMessageCodec());
			for (Class<?> value : ConstructorDetector.getTDConstructorsUnsafe().values()) {
				cluster.registerDefaultCodec(value, new TdMessageCodec(value));
			}
		}
		this.deliveryOptions = cluster.newDeliveryOpts().setLocalOnly(local);
		this.deliveryOptionsWithTimeout = cluster.newDeliveryOpts().setLocalOnly(local).setSendTimeout(30000);
	}

	public static Mono<AsyncTdMiddleEventBusClient> getAndDeployInstance(TdClusterManager clusterManager, String botAlias, String botAddress, boolean local) throws InitializationException {
		try {
			var instance = new AsyncTdMiddleEventBusClient(clusterManager);
			var options = clusterManager.newDeploymentOpts().setConfig(new JsonObject()
					.put("botAddress", botAddress)
					.put("botAlias", botAlias)
					.put("local", local));
			return MonoUtils.<String>executeAsFuture(promise -> {
				clusterManager.getVertx().deployVerticle(instance, options, promise);
			}).doOnNext(_v -> {
				logger.trace("Deployed verticle for bot address: " + botAddress);
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
		var local = config().getBoolean("local");
		if (local == null) {
			throw new IllegalArgumentException("local is not set!");
		}
		this.local = local;
		this.initTime = System.currentTimeMillis();

		CircuitBreaker startBreaker = CircuitBreaker.create("bot-" + botAddress + "-server-online-check-circuit-breaker", vertx,
				new CircuitBreakerOptions().setMaxFailures(1).setMaxRetries(4).setTimeout(30000)
		)
				.retryPolicy(policy -> 4000L)
				.openHandler(closed -> {
					logger.error("Circuit opened! " + botAddress);
				})
				.closeHandler(closed -> {
					logger.error("Circuit closed! " + botAddress);
				});

		startBreaker.execute(future -> {
			try {
				logger.debug("Requesting tdlib.remoteclient.clients.deploy.existing");
				cluster.getEventBus().publish("tdlib.remoteclient.clients.deploy", botAddress, deliveryOptions);


				logger.debug("Waiting for " + botAddress + ".readyToStart");
				AtomicBoolean alreadyReceived = new AtomicBoolean(false);
				this.readyToStartConsumer = cluster.getEventBus().consumer(botAddress + ".readyToStart", (Message<byte[]> pingMsg) -> {
					// Reply instantly
					pingMsg.reply(new byte[0]);

					if (!alreadyReceived.getAndSet(true)) {
						logger.debug("Received ping reply (succeeded)");
						logger.debug("Requesting " + botAddress + ".start");
						cluster
								.getEventBus()
								.request(botAddress + ".start", EMPTY, deliveryOptionsWithTimeout, startMsg -> {
									if (startMsg.succeeded()) {
										logger.debug("Requesting " + botAddress + ".isWorking");
										cluster
												.getEventBus()
												.request(botAddress + ".isWorking", EMPTY, deliveryOptionsWithTimeout, msg -> {
													if (msg.succeeded()) {
														this.listen().then(this.pipe()).timeout(Duration.ofSeconds(30)).subscribe(v -> {}, future::fail, future::complete);
													} else {
														future.fail(msg.cause());
													}
												});
									} else {
										future.fail(startMsg.cause());
									}
								});
					} else {
						// Already received
					}
				});
			} catch (Exception ex) {
				future.fail(ex);
			}
		})
				.onFailure(ex -> {
					logger.error("Failure when starting bot " + botAddress, ex);
					startPromise.fail(new InitializationException("Can't connect tdlib middle client to tdlib middle server!"));
				})
				.onSuccess(v -> startPromise.complete());
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		readyToStartConsumer.unregister(result -> {
			tdClosed.onNext(true);
			stopPromise.complete();
		});
	}

	private Mono<Void> listen() {
		// Nothing to listen for now
		return Mono.empty();
	}

	private Mono<Void> pipe() {
		var updates = this.requestUpdatesBatchFromNetwork()
				.repeatWhen(nFlux -> nFlux.takeWhile(n -> n > 0)) // Repeat when there is one batch with a flux of updates
				.takeUntilOther(tdClosed.distinct().filter(tdClosed -> tdClosed)) // Stop when closed
				.flatMap(batch -> batch)
				.onErrorResume(error -> {
					logger.error("Bot updates request failed! Marking as closed.", error);
					if (error.getMessage().contains("Timed out")) {
						return Flux.just(new Error(444, "CONNECTION_KILLED"));
					} else {
						return Flux.just(new Error(406, "INVALID_UPDATE"));
					}
				})
				.flatMap(update -> {
					return Mono.<TdApi.Object>create(sink -> {
						if (update.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
							var state = (UpdateAuthorizationState) update;
							if (state.authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR) {

								// Send tdClosed early to avoid errors
								tdClosed.onNext(true);

								this.getVertx().undeploy(this.deploymentID(), undeployed -> {
									if (undeployed.failed()) {
										logger.error("Error when undeploying td verticle", undeployed.cause());
									}
									sink.success(update);
								});
							} else {
								sink.success(update);
							}
						} else {
							sink.success(update);
						}
					});
				})
				.log("TdMiddle", Level.FINEST)
				.takeUntilOther(tdClosed.distinct().filter(tdClosed -> tdClosed)) // Stop when closed
				.publish()
				.autoConnect(1);

		updates.subscribe(t -> incomingUpdatesCo.onNext(Flux.just(t)),
				incomingUpdatesCo::onError,
				incomingUpdatesCo::onComplete
		);

		return Mono.empty();
	}

	private static class UpdatesBatchResult {
		public final Flux<TdApi.Object> updatesFlux;
		public final boolean completed;

		private UpdatesBatchResult(Flux<TdApi.Object> updatesFlux, boolean completed) {
			this.updatesFlux = updatesFlux;
			this.completed = completed;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", UpdatesBatchResult.class.getSimpleName() + "[", "]")
					.add("updatesFlux=" + updatesFlux)
					.add("completed=" + completed)
					.toString();
		}
	}

	private Mono<Flux<TdApi.Object>> requestUpdatesBatchFromNetwork() {
		return Mono
				.from(tdClosed.distinct())
				.single()
				.filter(tdClosed -> !tdClosed)
				.flatMap(_x -> Mono.<Flux<TdApi.Object>>create(sink -> {
					cluster.getEventBus().<TdOptionalList>request(botAddress + ".getNextUpdatesBlock",
							EMPTY,
							deliveryOptionsWithTimeout,
							msg -> {
								if (msg.failed()) {
									//if (System.currentTimeMillis() - initTime <= 30000) {
									//	// The serve has not been started
									//	sink.success(Flux.empty());
									//} else {
									//	// Timeout
									sink.error(msg.cause());
									//}
								} else {
									var result = msg.result();
									if (result.body() == null) {
										sink.success();
									} else {
										var resultBody = msg.result().body();
										if (resultBody.isSet()) {
											List<TdResult<TdApi.Object>> updates = resultBody.getValues();
											for (TdResult<TdApi.Object> updateObj : updates) {
												if (updateObj.succeeded()) {
													if (OUTPUT_REQUESTS) {
														System.out.println(" <- " + updateObj.result()
																.toString()
																.replace("\n", " ")
																.replace("\t", "")
																.replace("  ", "")
																.replace(" = ", "="));
													}
												} else {
													logger.error("Received an errored update",
															ResponseError.newResponseError("incoming update", botAlias, updateObj.cause())
													);
												}
											}
											sink.success(Flux.fromIterable(updates).filter(TdResult::succeeded).map(TdResult::result));
										} else {
											// the stream has ended
											sink.success();
										}
									}
								}
							}
					);
				}));
	}

	@Override
	public Flux<TdApi.Object> getUpdates() {
		return incomingUpdatesCo.filter(Objects::nonNull).flatMap(v -> v);
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {

		var req = new ExecuteObject(executeDirectly, request);
		if (OUTPUT_REQUESTS) {
			System.out.println(" -> " + request.toString()
					.replace("\n", " ")
					.replace("\t", "")
					.replace("  ", "")
					.replace(" = ", "="));
		}

		return Mono.from(tdClosed.distinct()).single().filter(tdClosed -> !tdClosed).<TdResult<T>>flatMap((_x) -> Mono.create(sink -> {
			try {
				cluster
						.getEventBus()
						.request(botAddress + ".execute",
								req,
								deliveryOptions,
								(AsyncResult<Message<TdResultMessage>> event) -> {
									try {
										if (event.succeeded()) {
											if (event.result().body() == null) {
												sink.error(new NullPointerException("Response is empty"));
											} else {
												sink.success(Objects.requireNonNull(event.result().body()).toTdResult());
											}
										} else {
											sink.error(ResponseError.newResponseError(request, botAlias, event.cause()));
										}
									} catch (Throwable t) {
										sink.error(t);
									}
								}
						);
			} catch (Throwable t) {
				sink.error(t);
			}
		})).<TdResult<T>>switchIfEmpty(Mono.defer(() -> Mono.just(TdResult.<T>failed(new TdApi.Error(500,
				"Client is closed or response is empty"
		))))).<TdResult<T>>handle((response, sink) -> {
			try {
				Objects.requireNonNull(response);
				if (OUTPUT_REQUESTS) {
					System.out.println(
							" <- " + response.toString().replace("\n", " ").replace("\t", "").replace("  ", "").replace(" = ", "="));
				}
				sink.next(response);
			} catch (Exception e) {
				sink.error(e);
			}
		}).switchIfEmpty(Mono.fromSupplier(() -> {
			return TdResult.failed(new TdApi.Error(500, "Client is closed or response is empty"));
		}));
	}
}
