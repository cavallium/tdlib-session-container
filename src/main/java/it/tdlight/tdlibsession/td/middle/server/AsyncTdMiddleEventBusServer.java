package it.tdlight.tdlibsession.td.middle.server;

import static it.tdlight.tdlibsession.td.middle.client.AsyncTdMiddleEventBusClient.OUTPUT_REQUESTS;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import it.tdlight.common.ConstructorDetector;
import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdExecuteObjectMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdOptListMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdOptionalList;
import it.tdlight.tdlibsession.td.middle.TdResultMessageCodec;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AsyncTdMiddleEventBusServer extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusServer.class);

	private static final byte[] EMPTY = new byte[0];
	// todo: restore duration to 2 seconds instead of 10 millis, when the bug of tdlight double queue wait is fixed
	public static final Duration WAIT_DURATION = Duration.ofSeconds(1);// Duration.ofMillis(10);
	// If you enable this the poll will wait up to 100 additional milliseconds between each poll, if the server is remote
	private static final boolean ENABLE_MINIMUM_POLL_WAIT_INTERVAL = false;

	private final TdClusterManager cluster;
	private final AsyncTdDirectOptions tdOptions;

	private String botAlias;
	private String botAddress;
	private boolean local;

	protected AsyncTdDirectImpl td;
	private final Scheduler tdSrvPoll;
	/**
	 * Value is not important, emits when a request is received
	 */
	private final List<Consumer<Promise<Void>>> onBeforeStopListeners = new CopyOnWriteArrayList<>();
	private final List<Consumer<Promise<Void>>> onAfterStopListeners = new CopyOnWriteArrayList<>();
	private MessageConsumer<?> startConsumer;
	private MessageConsumer<byte[]> isWorkingConsumer;
	private MessageConsumer<byte[]> getNextUpdatesBlockConsumer;
	private MessageConsumer<ExecuteObject> executeConsumer;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public AsyncTdMiddleEventBusServer(TdClusterManager clusterManager) {
		this.cluster = clusterManager;
		this.tdOptions = new AsyncTdDirectOptions(WAIT_DURATION, 1000);
		this.tdSrvPoll = Schedulers.newSingle("TdSrvPoll");
		if (cluster.registerDefaultCodec(TdOptionalList.class, new TdOptListMessageCodec())) {
			cluster.registerDefaultCodec(ExecuteObject.class, new TdExecuteObjectMessageCodec());
			cluster.registerDefaultCodec(TdResultMessage.class, new TdResultMessageCodec());
			for (Class<?> value : ConstructorDetector.getTDConstructorsUnsafe().values()) {
				cluster.registerDefaultCodec(value, new TdMessageCodec(value));
			}
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
		this.td = new AsyncTdDirectImpl(botAlias);

		AtomicBoolean alreadyDeployed = new AtomicBoolean(false);
		this.startConsumer = cluster.getEventBus().consumer(botAddress + ".start", (Message<byte[]> msg) -> {
			if (alreadyDeployed.compareAndSet(false, true)) {
				this.listen().then(this.pipe()).then(Mono.<Void>create(registrationSink -> {
					this.isWorkingConsumer = cluster.getEventBus().consumer(botAddress + ".isWorking", (Message<byte[]> workingMsg) -> {
						workingMsg.reply(EMPTY, cluster.newDeliveryOpts().setLocalOnly(local));
					});
					this.isWorkingConsumer.completionHandler(MonoUtils.toHandler(registrationSink));
				})).subscribeOn(this.tdSrvPoll)
				.subscribe(v -> {}, ex -> {
					logger.info(botAddress + " server deployed and started. succeeded: false");
					logger.error(ex.getLocalizedMessage(), ex);
					msg.fail(500, ex.getLocalizedMessage());
				}, () -> {
					logger.info(botAddress + " server deployed and started. succeeded: true");
					msg.reply(EMPTY);
				});
			} else {
				msg.reply(EMPTY);
			}
		});
		startConsumer.completionHandler(h -> {
			logger.info(botAddress + " server deployed. succeeded: " + h.succeeded());
			if (h.succeeded()) {
				startPromise.complete(h.result());
			} else {
				startPromise.fail(h.cause());
			}
		});

		logger.debug("Sending " + botAddress + ".readyToStart");
		cluster.getEventBus().send(botAddress + ".readyToStart", EMPTY, cluster.newDeliveryOpts().setSendTimeout(30000));
	}

	public void onBeforeStop(Consumer<Promise<Void>> r) {
		this.onBeforeStopListeners.add(r);
	}

	public void onAfterStop(Consumer<Promise<Void>> r) {
		this.onAfterStopListeners.add(r);
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		runAll(onBeforeStopListeners, onBeforeStopHandler -> {
			if (onBeforeStopHandler.failed()) {
				logger.error("A beforeStop listener failed: "+ onBeforeStopHandler.cause());
			}

			Mono.create(sink -> this.isWorkingConsumer.unregister(result -> {
				if (result.failed()) {
					logger.error("Can't unregister consumer", result.cause());
				}
				this.startConsumer.unregister(result2 -> {
					if (result2.failed()) {
						logger.error("Can't unregister consumer", result2.cause());
					}

					this.getNextUpdatesBlockConsumer.unregister(result3 -> {
						if (result3.failed()) {
							logger.error("Can't unregister consumer", result3.cause());
						}

						this.executeConsumer.unregister(result4 -> {
							if (result4.failed()) {
								logger.error("Can't unregister consumer", result4.cause());
							}

							sink.success();
						});
					});
				});
			})).doFinally(signalType -> {
				logger.info("TdMiddle verticle \"" + botAddress + "\" stopped");

				runAll(onAfterStopListeners, onAfterStopHandler -> {
					if (onAfterStopHandler.failed()) {
						logger.error("An afterStop listener failed: " + onAfterStopHandler.cause());
					}

					stopPromise.complete();
				});
			}).subscribeOn(this.tdSrvPoll).subscribe(v -> {}, ex -> {
				logger.error("Error when stopping", ex);
			}, () -> {});
		});
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

	private Mono<Void> listen() {
		return Mono.<Void>create(registrationSink -> {

			this.executeConsumer = cluster.getEventBus().<ExecuteObject>consumer(botAddress + ".execute", (Message<ExecuteObject> msg) -> {
				try {
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
					td
							.execute(msg.body().getRequest(), msg.body().isExecuteDirectly())
							.switchIfEmpty(Mono.fromSupplier(() -> {
								return TdResult.failed(new TdApi.Error(500, "Received null response"));
							}))
							.handle((response, sink) -> {
								try {
									msg.reply(new TdResultMessage(response.result(), response.cause()),
											cluster.newDeliveryOpts().setLocalOnly(local)
									);
									sink.next(response);
								} catch (Exception ex) {
									sink.error(ex);
								}
							}).subscribeOn(this.tdSrvPoll)
							.subscribe(response -> {}, ex -> {
								logger.error("Error when processing a request", ex);
								msg.fail(500, ex.getLocalizedMessage());
							});
				} catch (ClassCastException ex) {
					logger.error("Error when deserializing a request", ex);
					msg.fail(500, ex.getMessage());
				}
			});
			executeConsumer.completionHandler(MonoUtils.toHandler(registrationSink));

		});
	}

	private void undeploy(Runnable whenUndeployed) {
		vertx.undeploy(deploymentID(), undeployed -> {
			if (undeployed.failed()) {
				logger.error("Error when undeploying td verticle", undeployed.cause());
			}
			whenUndeployed.run();
		});
	}

	private Mono<Void> pipe() {
		return Mono.create(registeredSink -> {
			Many<Boolean> getNextUpdatesBlockTrigger = Sinks.many().replay().latestOrDefault(true);
			Flux<Message<byte[]>> getNextUpdatesBlockFlux = Flux.create(sink -> {
				this.getNextUpdatesBlockConsumer = cluster.getEventBus().consumer(botAddress + ".getNextUpdatesBlock", (Message<byte[]> msg) -> {
					getNextUpdatesBlockTrigger.tryEmitNext(true);
					sink.next(msg);
				});
				registeredSink.success();
			});

			Empty<Boolean> needClose = Sinks.empty();
			Flux<TdOptionalList> updatesFlux = Flux.mergeSequential(td.receive(tdOptions)
							.doOnSubscribe(s -> {
								// After 60 seconds of not receiving request for updates, dispose td flux assuming the middle client died.
								getNextUpdatesBlockTrigger.asFlux().timeout(Duration.ofSeconds(30), timeout -> {
									needClose.tryEmitEmpty();
									s.cancel();
								}).subscribeOn(this.tdSrvPoll).subscribe(v -> {}, ex -> {
									logger.error("Error when signalling that the next update block request has been received", ex);
								}, () -> {
									needClose.tryEmitEmpty();
								});
							})
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
							.bufferTimeout(1000, local ? Duration.ofMillis(1) : Duration.ofMillis(100))
							.windowTimeout(1, Duration.ofSeconds(5))
							.flatMap(w -> w.defaultIfEmpty(Collections.emptyList()))
							.map(updatesGroup -> new TdOptionalList(true, updatesGroup)),
					Mono.fromSupplier(() -> {
						return new TdOptionalList(false, Collections.emptyList());
					})
			);

			Flux.zip(updatesFlux, getNextUpdatesBlockFlux)
					.subscribeOn(this.tdSrvPoll)
					.subscribe(tuple -> {
						var results = tuple.getT1();
						var messageHandle = tuple.getT2();

						if (!results.isSet()) {
							System.out.println("<=: end (1)");
						}

						messageHandle.reply(results, cluster.newDeliveryOpts().setLocalOnly(local));
					}, error -> logger.error("Error when receiving or forwarding updates", error), () -> {
						needClose.tryEmitEmpty();
					});
			needClose.asMono().subscribeOn(this.tdSrvPoll).subscribe(v_ -> {}, e -> {}, () -> {
				this.undeploy(() -> {});
			});
		});

	}
}
