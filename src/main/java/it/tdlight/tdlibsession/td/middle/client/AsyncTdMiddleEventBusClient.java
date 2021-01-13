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
import it.tdlight.tdlibsession.EventBusFlux;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdExecuteObjectMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdResultMessageCodec;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AsyncTdMiddleEventBusClient extends AbstractVerticle implements AsyncTdMiddle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusClient.class );

	public static final boolean OUTPUT_REQUESTS = false;
	public static final byte[] EMPTY = new byte[0];

	private final Scheduler tdMiddleScheduler = Schedulers.single();
	private final Many<Boolean> tdClosed = Sinks.many().replay().latestOrDefault(false);
	private final DeliveryOptions deliveryOptions;
	private final DeliveryOptions deliveryOptionsWithTimeout;

	private TdClusterManager cluster;

	private String botAddress;
	private String botAlias;
	private boolean local;
	private long initTime;
	private MessageConsumer<byte[]> readyToStartConsumer;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public AsyncTdMiddleEventBusClient(TdClusterManager clusterManager) {
		cluster = clusterManager;
		if (cluster.registerDefaultCodec(TdResultList.class, new TdResultListMessageCodec())) {
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
														this.listen()
																.timeout(Duration.ofSeconds(30))
																.subscribe(v -> {}, future::fail, future::complete);
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
			tdClosed.tryEmitNext(true);
			stopPromise.complete();
		});
	}

	private Mono<Void> listen() {
		// Nothing to listen for now
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

	@Override
	public Flux<TdApi.Object> receive() {
		var fluxCodec = new TdResultListMessageCodec();
		return Mono.from(tdClosed.asFlux()).single().filter(tdClosed -> !tdClosed).flatMapMany(_closed -> EventBusFlux
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
					logger.error("Bot updates request failed! Marking as closed.", error);
					if (error.getMessage().contains("Timed out")) {
						return Flux.just(TdResult.failed(new Error(444, "CONNECTION_KILLED")));
					} else {
						return Flux.just(TdResult.failed(new Error(406, "INVALID_UPDATE")));
					}
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
							tdClosed.tryEmitNext(true);
						}
					}
				}));
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

		return Mono.from(tdClosed.asFlux()).single().filter(tdClosed -> !tdClosed).<TdResult<T>>flatMap((_x) -> Mono.create(sink -> {
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
