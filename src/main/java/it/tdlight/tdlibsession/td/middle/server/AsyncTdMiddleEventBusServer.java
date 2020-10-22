package it.tdlight.tdlibsession.td.middle.server;

import static it.tdlight.tdlibsession.td.middle.client.AsyncTdMiddleEventBusClient.OUTPUT_REQUESTS;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import it.tdlight.common.ConstructorDetector;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
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

	private String botAlias;
	private String botAddress;
	private boolean local;

	protected final ReplayProcessor<Boolean> tdClosed = ReplayProcessor.cacheLastOrDefault(false);
	protected AsyncTdDirectImpl td;
	protected final LinkedBlockingQueue<AsyncResult<TdResult<Update>>> queue = new LinkedBlockingQueue<>();
	private final Scheduler tdSrvPoll;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public AsyncTdMiddleEventBusServer(TdClusterManager clusterManager) {
		this.cluster = clusterManager;
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

		cluster.getEventBus().consumer(botAddress + ".ping", (Message<byte[]> msg) -> {
			logger.error("Received ping. Replying...");
			msg.reply(EMPTY);
			logger.error("Replied.");
		});

		AtomicBoolean alreadyDeployed = new AtomicBoolean(false);
		cluster.getEventBus().consumer(botAddress + ".start", (Message<byte[]> msg) -> {
			if (alreadyDeployed.compareAndSet(false, true)) {
				td.initializeClient()
						.then(this.listen())
						.then(this.pipe())
						.then(Mono.<Void>create(registrationSink -> {

							cluster.getEventBus().consumer(botAddress + ".isWorking", (Message<byte[]> workingMsg) -> {
								workingMsg.reply(EMPTY, cluster.newDeliveryOpts().setLocalOnly(local));
							}).completionHandler(MonoUtils.toHandler(registrationSink));

						}))
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
		}).completionHandler(h -> {
			logger.info(botAddress + " server deployed. succeeded: " + h.succeeded());
			if (h.succeeded()) {
				startPromise.complete(h.result());
			} else {
				startPromise.fail(h.cause());
			}
		});
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

	private Mono<Void> listen() {
		return Mono.<Void>create(registrationSink -> {
			cluster.getEventBus().consumer(botAddress + ".getNextUpdatesBlock", (Message<byte[]> msg) -> {
				// Run only if tdlib is not closed
				Mono.from(tdClosed).single().filter(tdClosedVal -> !tdClosedVal)
						// Get a list of updates
						.flatMap(_v -> Mono
										.<List<AsyncResult<TdResult<Update>>>>fromSupplier(() -> {
											// When a request is asked, read up to 1000 available updates in the queue
											long requestTime = System.currentTimeMillis();
											ArrayList<AsyncResult<TdResult<Update>>> updatesBatch = new ArrayList<>();
											try {
												// Block until an update is found or 5 seconds passed
												var item = queue.poll(5, TimeUnit.SECONDS);
												if (item != null) {
													updatesBatch.add(item);
													queue.drainTo(updatesBatch, local ? 999 : 998);

													if (ENABLE_MINIMUM_POLL_WAIT_INTERVAL) {
														if (!local) {
															var item2 = queue.poll(100, TimeUnit.MILLISECONDS);
															if (item2 != null) {
																updatesBatch.add(item2);
																queue.drainTo(updatesBatch, Math.max(0, 1000 - updatesBatch.size()));
															}
														}
													}
												}
											} catch (InterruptedException ex) {
												// polling cancelled, expected sometimes
											}
											// Return the updates found, can be an empty list
											return updatesBatch;
										})
								// Subscribe on td server poll scheduler
								.subscribeOn(tdSrvPoll)
								// Filter out empty updates lists
								.filter(updates -> !updates.isEmpty())
								.repeatWhen(s -> s
										// Take until an update is received
										.takeWhile(n -> n == 0)
										// Take until tdClosed is true
										.takeUntilOther(tdClosed.filter(closed -> closed).take(1).single())
								)
								// Take only one update list
								.take(1)
								// If 5 seconds pass, return a list with 0 updates
								.timeout(Duration.ofSeconds(5), Mono.just(List.of()))
								// Return 1 list or 0 lists
								.singleOrEmpty()
						)
						.flatMap(receivedList -> {
							return Flux.fromIterable(receivedList).flatMap(result -> {
								if (result.succeeded()) {
									var received = result.result();
									if (OUTPUT_REQUESTS) {
										System.out.println("<=: " + received
												.toString()
												.replace("\n", " ")
												.replace("\t", "")
												.replace("  ", "")
												.replace(" = ", "="));
									}
									return Mono.create(sink -> {
										if (received.succeeded() && received.result().getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
											var authState = (UpdateAuthorizationState) received.result();
											if (authState.authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR) {
												tdClosed.onNext(true);
												vertx.undeploy(deploymentID(), undeployed -> {
													if (undeployed.failed()) {
														logger.error("Error when undeploying td verticle", undeployed.cause());
													}
													sink.success();
												});
											} else {
												sink.success();
											}
										} else {
											sink.success();
										}
									}).then(Mono.<TdResult<Update>>create(sink -> {
										sink.success(received);
									}));
								} else {
									logger.error("Received an error update", result.cause());
									return Mono.empty();
								}
							}).collectList().map(list -> new TdOptionalList(true, list));
						})
						.defaultIfEmpty(new TdOptionalList(false, Collections.emptyList()))
						.subscribeOn(tdSrvPoll)
						.subscribe(v -> {
							msg.reply(v);
						}, ex -> {
							logger.error("Error when processing a 'receiveUpdates' request", ex);
							msg.fail(500, ex.getLocalizedMessage());
						}, () -> {});
			}).completionHandler(MonoUtils.toHandler(registrationSink));

		}).then(Mono.<Void>create(registrationSink -> {

			cluster.getEventBus().<ExecuteObject>consumer(botAddress + ".execute", (Message<ExecuteObject> msg) -> {
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
							})
							.subscribe(response -> {}, ex -> {
								logger.error("Error when processing a request", ex);
								msg.fail(500, ex.getLocalizedMessage());
							});
				} catch (ClassCastException ex) {
					logger.error("Error when deserializing a request", ex);
					msg.fail(500, ex.getMessage());
				}
			}).completionHandler(MonoUtils.toHandler(registrationSink));

		}));
	}

	private Mono<Void> pipe() {
		return Mono.fromCallable(() -> {
			td
					.getUpdates(WAIT_DURATION, 1000)
					.bufferTimeout(1000, local ? Duration.ofMillis(1) : Duration.ofMillis(100))
					.subscribe(nextItems -> {
						queue.addAll(nextItems);
					});
			return (Void) null;
		});
	}
}
