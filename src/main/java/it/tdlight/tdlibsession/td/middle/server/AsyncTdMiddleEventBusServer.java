package it.tdlight.tdlibsession.td.middle.server;

import static it.tdlight.tdlibsession.td.middle.client.AsyncTdMiddleEventBusClient.OUTPUT_REQUESTS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.EventBusFlux;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.util.function.Tuples;

public class AsyncTdMiddleEventBusServer {

	// Static values
	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusServer.class);
	public static final byte[] EMPTY = new byte[0];
	public static final Duration WAIT_DURATION = Duration.ofSeconds(1);

	// Values configured from constructor
	private final TdClusterManager cluster;
	private final AsyncTdDirectOptions tdOptions;

	// Variables configured by the user at startup
	private final One<String> botAlias = Sinks.one();
	private final One<String> botAddress = Sinks.one();
	private final One<Boolean> local = Sinks.one();
	private final Many<Consumer<Promise<Void>>> onBeforeStopListeners = Sinks.many().replay().all();
	private final Many<Consumer<Promise<Void>>> onAfterStopListeners = Sinks.many().replay().all();

	// Variables configured at startup
	private final One<AsyncTdDirectImpl> td = Sinks.one();
	private final One<MessageConsumer<byte[]>> startConsumer = Sinks.one();
	private final One<MessageConsumer<byte[]>> isWorkingConsumer = Sinks.one();
	private final One<MessageConsumer<ExecuteObject>> executeConsumer = Sinks.one();

	@SuppressWarnings({"unchecked", "rawtypes"})
	public AsyncTdMiddleEventBusServer(TdClusterManager clusterManager) {
		this.cluster = clusterManager;
		this.tdOptions = new AsyncTdDirectOptions(WAIT_DURATION, 100);
	}

	public Mono<AsyncTdMiddleEventBusServer> start(String botAddress, String botAlias, boolean local) {
		return Mono.<Void>create(sink -> {
			if (botAddress == null || botAddress.isEmpty()) {
				sink.error(new IllegalArgumentException("botAddress is not set!"));
				return;
			}
			if (this.botAddress.tryEmitValue(botAddress).isFailure()) {
				sink.error(new IllegalStateException("Failed to set botAddress"));
				return;
			}
			if (botAlias == null || botAlias.isEmpty()) {
				sink.error(new IllegalArgumentException("botAlias is not set!"));
				return;
			}
			if (this.botAlias.tryEmitValue(botAlias).isFailure()) {
				sink.error(new IllegalStateException("Failed to set botAlias"));
				return;
			}
			if (this.local.tryEmitValue(local).isFailure()) {
				sink.error(new IllegalStateException("Failed to set local"));
				return;
			}
			var td = new AsyncTdDirectImpl(botAlias);
			if (this.td.tryEmitValue(td).isFailure()) {
				sink.error(new IllegalStateException("Failed to set td instance"));
				return;
			}
			if (this.onBeforeStopListeners.tryEmitComplete().isFailure()) {
				sink.error(new IllegalStateException("Failed to finalize \"before stop\" listeners"));
				return;
			}
			if (this.onAfterStopListeners.tryEmitComplete().isFailure()) {
				sink.error(new IllegalStateException("Failed to finalize \"after stop\" listeners"));
				return;
			}

			AtomicBoolean alreadyDeployed = new AtomicBoolean(false);
			var startConsumer = cluster.getEventBus().<byte[]>consumer(botAddress + ".start");
			if (this.startConsumer.tryEmitValue(startConsumer).isSuccess()) {
				startConsumer.handler((Message<byte[]> msg) -> {
					if (alreadyDeployed.compareAndSet(false, true)) {
						startConsumer.unregister(startConsumerUnregistered -> {
							if (startConsumerUnregistered.succeeded()) {
								onSuccessfulStartRequest(msg, td, botAddress, botAlias, local);
							} else {
								logger.error("Failed to unregister start consumer");
							}
						});
					} else {
						msg.reply(EMPTY);
					}
				});
				startConsumer.completionHandler(h -> {
					logger.info(botAddress + " server deployed. succeeded: " + h.succeeded());
					if (h.succeeded()) {
						var readyToStartOpts = cluster.newDeliveryOpts().setSendTimeout(30000);
						logger.debug("Sending " + botAddress + ".readyToStart");
						cluster.getEventBus().request(botAddress + ".readyToStart", EMPTY, readyToStartOpts, msg -> sink.success());
					} else {
						sink.error(h.cause());
					}
				});
			} else {
				sink.error(new IllegalStateException("Failed to set startConsumer"));
			}
		}).thenReturn(this);
	}

	private void onSuccessfulStartRequest(Message<byte[]> msg, AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		this.listen(td, botAddress, botAlias, local).then(this.pipe(td, botAddress, botAlias, local)).then(Mono.<Void>create(registrationSink -> {
			var isWorkingConsumer = cluster.getEventBus().<byte[]>consumer(botAddress + ".isWorking");
			if (this.isWorkingConsumer.tryEmitValue(isWorkingConsumer).isSuccess()) {
				isWorkingConsumer.handler((Message<byte[]> workingMsg) -> {
					workingMsg.reply(EMPTY, cluster.newDeliveryOpts().setLocalOnly(local));
				});
				isWorkingConsumer.completionHandler(MonoUtils.toHandler(registrationSink));
			} else {
				logger.error("Failed to set isWorkingConsumer");
				msg.fail(500, "Failed to set isWorkingConsumer");
			}
		})).subscribe(v -> {}, ex -> {
			logger.error("Deploy and start of bot \"" + botAlias + "\": ❌ Failed", ex);
			msg.fail(500, ex.getLocalizedMessage());
		}, () -> {
			logger.info("Deploy and start of bot \"" + botAlias + "\": ✅ Succeeded");
			msg.reply(EMPTY);
		});
	}

	/**
	 * Register a before stop listener
	 * @param eventListener listener
	 * @return success if the listener has been registered correctly
	 */
	public EmitResult onBeforeStop(Consumer<Promise<Void>> eventListener, boolean crashOnFail) {
		if (crashOnFail) {
			return this.onBeforeStopListeners.tryEmitNext(eventListener);
		} else {
			return this.onBeforeStopListeners.tryEmitNext(promise -> {
				Promise<Void> falliblePromise = Promise.promise();
				falliblePromise.future().onComplete(result -> {
					if (result.failed()) {
						logger.warn("A beforeStop listener failed. Ignored error", result.cause());
					}
					promise.complete();
				});
				eventListener.accept(falliblePromise);

			});
		}
	}

	/**
	 * Register an after stop listener
	 * @param eventListener listener
	 * @return success if the listener has been registered correctly
	 */
	public EmitResult onAfterStop(Consumer<Promise<Void>> eventListener, boolean crashOnFail) {
		if (crashOnFail) {
			return this.onAfterStopListeners.tryEmitNext(eventListener);
		} else {
			return this.onAfterStopListeners.tryEmitNext(promise -> {
				Promise<Void> falliblePromise = Promise.promise();
				falliblePromise.future().onComplete(result -> {
					if (result.failed()) {
						logger.warn("An afterStop listener failed. Ignored error", result.cause());
					}
					promise.complete();
				});
				eventListener.accept(falliblePromise);

			});
		}
	}

	private void runAll(List<Consumer<Promise<Void>>> actions, Handler<AsyncResult<Void>> resultHandler) {
		if (actions.isEmpty()) {
			resultHandler.handle(Future.succeededFuture());
		} else {
			var firstAction = actions.remove(0);
			Promise<Void> promise = Promise.promise();
			firstAction.accept(promise);
			promise.future().onComplete(handler -> {
				if (handler.succeeded()) {
					runAll(new ArrayList<>(actions), resultHandler);
				} else {
					resultHandler.handle(Future.failedFuture(handler.cause()));
				}
			});
		}
	}

	private Mono<Void> listen(AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		return Mono.<Void>create(registrationSink -> {
			MessageConsumer<ExecuteObject> executeConsumer = cluster.getEventBus().consumer(botAddress + ".execute");
			if (this.executeConsumer.tryEmitValue(executeConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set executeConsumer"));
				return;
			}

			Flux.<Message<ExecuteObject>>create(sink -> {
				executeConsumer.handler(sink::next);
				executeConsumer.completionHandler(MonoUtils.toHandler(registrationSink));
			})
					.doOnNext(msg -> {
						if (OUTPUT_REQUESTS) {
							System.out.println(":=> " + msg
									.body()
									.getRequest()
									.toString()
									.replace("\n", " ")
									.replace("\t", "")
									.replace("  ", "")
									.replace(" = ", "="));
						}
					})
					.flatMap(msg -> td
							.execute(msg.body().getRequest(), msg.body().isExecuteDirectly())
							.map(result -> Tuples.of(msg, result)))
					.handle((tuple, sink) -> {
						var msg = tuple.getT1();
						var response = tuple.getT2();
						var replyOpts = cluster.newDeliveryOpts().setLocalOnly(local);
						var replyValue = new TdResultMessage(response.result(), response.cause());
						try {
							msg.reply(replyValue, replyOpts);
							sink.next(response);
						} catch (Exception ex) {
							msg.fail(500, ex.getLocalizedMessage());
							sink.error(ex);
						}
					})
					.then()
					.doOnError(ex -> {
						logger.error("Error when processing a request", ex);
					})
					.subscribe();
		});
	}

	private void undeploy(Runnable whenUndeployed) {
		botAlias.asMono().single().flatMap(botAlias -> {
			logger.info("Undeploy of bot \"" + botAlias + "\": stopping");
			return onBeforeStopListeners.asFlux()
					.collectList()
					.single()
					.flatMap(onBeforeStopListeners ->
							Mono.<Void>create(sink -> runAll(onBeforeStopListeners, MonoUtils.toHandler(sink)))
									.doOnError(ex -> logger.error("A beforeStop listener failed", ex)))
					.then(Flux
							.merge(unregisterConsumerOrLog(this.isWorkingConsumer.asMono(), "isWorkingConsumer"),
									unregisterConsumerOrLog(this.startConsumer.asMono(), "isWorkingConsumer"),
									unregisterConsumerOrLog(this.executeConsumer.asMono(), "isWorkingConsumer"))
							.then())
					.then(onAfterStopListeners.asFlux().collectList())
					.single()
					.doOnNext(onAfterStopListeners -> {
						logger.info("TdMiddle verticle \"" + botAddress + "\" stopped");

						runAll(onAfterStopListeners, onAfterStopHandler -> {
							if (onAfterStopHandler.failed()) {
								logger.error("An afterStop listener failed: " + onAfterStopHandler.cause());
							}

							logger.info("Undeploy of bot \"" + botAlias + "\": stopped");
							whenUndeployed.run();
						});
					});
		})
				.doOnError(ex -> logger.error("Error when stopping", ex))
				.subscribe();
	}

	private <T> Mono<Void> unregisterConsumerOrLog(Mono<MessageConsumer<T>> consumerMono, String consumerName) {
		return consumerMono
				.flatMap(consumer -> Mono
						.<Void>create(sink -> consumer.unregister(MonoUtils.toHandler(sink)))
						.onErrorResume(ex -> Mono.fromRunnable(() -> {
							logger.error("Can't unregister consumer \"" + consumerName + "\"", ex);
						})));
	}

	private Mono<Void> pipe(AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		var updatesFlux = td
				.receive(tdOptions)
				.doOnNext(update -> {
					if (OUTPUT_REQUESTS) {
						System.out.println("<=: " + update
								.toString()
								.replace("\n", " ")
								.replace("\t", "")
								.replace("  ", "")
								.replace(" = ", "="));
					}
				})
				.flatMap(item -> Mono.<TdResult<TdApi.Object>>create(sink -> {
					if (item.succeeded()) {
						var tdObject = item.result();
						if (tdObject instanceof Update) {
							var tdUpdate = (Update) tdObject;
							if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
								var tdUpdateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
								if (tdUpdateAuthorizationState.authorizationState.getConstructor()
										== AuthorizationStateClosed.CONSTRUCTOR) {
									logger.debug("Undeploying after receiving AuthorizationStateClosed");
									this.undeploy(() -> sink.success(item));
									return;
								}
							}
						} else if (tdObject instanceof Error) {
							// An error in updates means that a fatal error occurred
							logger.debug("Undeploying after receiving a fatal error");
							this.undeploy(() -> sink.success(item));
							return;
						}
						sink.success(item);
					}
				}))
				.bufferTimeout(tdOptions.getEventsSize(), local ? Duration.ofMillis(1) : Duration.ofMillis(100))
				.windowTimeout(1, Duration.ofSeconds(5))
				.flatMap(w -> w.defaultIfEmpty(Collections.emptyList()))
				.map(TdResultList::new).doOnTerminate(() -> {
					if (OUTPUT_REQUESTS) {
						System.out.println("<=: end (3)");
					}
				});
		var fluxCodec = new TdResultListMessageCodec();
		var tuple = EventBusFlux.<TdResultList>serve(updatesFlux,
				cluster.getEventBus(),
				botAddress + ".updates",
				cluster.newDeliveryOpts().setLocalOnly(local),
				fluxCodec,
				Duration.ofSeconds(30)
		);
		var served = tuple.getT1();
		var fatalError = tuple.getT2();
		//noinspection CallingSubscribeInNonBlockingScope
		fatalError
				.doOnNext(e -> logger.warn("Undeploying after a fatal error in a served flux"))
				.flatMap(error -> td.execute(new TdApi.Close(), false))
				.doOnError(ex -> logger.error("Unexpected error", ex))
				.subscribe();
				return served.doOnSubscribe(s -> {
					logger.debug("Preparing to serve bot \"" + botAlias + "\" updates flux...");
				}).doOnSuccess(v -> {
					logger.debug("Ready to serve bot \"" + botAlias + "\" updates flux");
				});
	}
}
