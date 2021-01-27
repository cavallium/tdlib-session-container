package it.tdlight.tdlibsession;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import it.tdlight.utils.MonoUtils;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class EventBusFlux {
	private static final Logger logger = LoggerFactory.getLogger(EventBusFlux.class);

	private static final byte[] EMPTY = new byte[0];

	public static <T> void registerFluxCodec(EventBus eventBus, MessageCodec<T, T> itemsCodec) {
		var signalsCodec = new SignalMessageCodec<T>(itemsCodec);
		try {
			eventBus.registerCodec(signalsCodec);
		} catch (IllegalStateException ex) {
			if (!ex.getMessage().contains("Already a codec registered with name")) {
				throw ex;
			}
		}
	}

	/**
	 * If the flux is fast and you are on a network, please do this:
	 *
	 * <pre>flux
  .bufferTimeout(Duration.ofMillis(100))
  .windowTimeout(1, Duration.ofSeconds(5))
  .flatMap(w -> w.defaultIfEmpty(Collections.emptyList()))</pre>
	 *
	 * @return tuple. T1 = flux served, T2 = error that caused cancelling of the subscription
	 */
	public static <T> Tuple2<Mono<Void>, Mono<Throwable>> serve(Flux<T> flux,
			EventBus eventBus,
			String fluxAddress,
			DeliveryOptions baseDeliveryOptions,
			MessageCodec<T, T> itemsCodec,
			Duration connectionTimeout) {
		var signalsCodec = new SignalMessageCodec<T>(itemsCodec);
		EventBusFlux.registerFluxCodec(eventBus, itemsCodec);
		var deliveryOptions = new DeliveryOptions(baseDeliveryOptions)
				.setSendTimeout(connectionTimeout.toMillis());
		var signalDeliveryOptions = new DeliveryOptions(deliveryOptions)
				.setCodecName(signalsCodec.name());
		AtomicInteger subscriptionsCount = new AtomicInteger();
		One<Throwable> fatalErrorSink = Sinks.one();
		var servedMono =  Mono.<Void>create(sink -> {
			MessageConsumer<byte[]> subscribe = eventBus.consumer(fluxAddress + ".subscribe");

			subscribe.handler(msg -> {
				subscribe.unregister(subscribeUnregistered -> {
					if (subscribeUnregistered.succeeded()) {
						if (subscriptionsCount.incrementAndGet() > 1) {
							subscriptionsCount.decrementAndGet();
							logger.error("Another client tried to connect to the same flux. Rejecting the request.");
							msg.fail(500, "This flux is already in use!");
							return;
						}
						long subscriptionId = 0;
						var subscriptionAddress = fluxAddress + "." + subscriptionId;

						MessageConsumer<byte[]> subscriptionReady = eventBus.consumer(subscriptionAddress + ".subscriptionReady");
						MessageConsumer<byte[]> dispose = eventBus.consumer(subscriptionAddress + ".dispose");
						MessageConsumer<byte[]> ping = eventBus.consumer(subscriptionAddress + ".ping");
						MessageConsumer<byte[]> cancel = eventBus.consumer(subscriptionAddress + ".cancel");

						subscriptionReady.<Long>handler(subscriptionReadyMsg -> {
							subscriptionReady.unregister(subscriptionReadyUnregistered -> {
								if (subscriptionReadyUnregistered.succeeded()) {
									AtomicReference<Disposable> atomicSubscription = new AtomicReference<>(null);
									var subscription = flux
											.onErrorResume(error -> Mono
													.<Message<T>>create(errorSink -> {
														var responseHandler = MonoUtils.toHandler(errorSink);
														eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onError(error), signalDeliveryOptions, responseHandler);
													})
													.then(Mono.empty())
											)
											.flatMapSequential(item -> Mono.<Message<T>>create(itemSink -> {
												var responseHandler = MonoUtils.toHandler(itemSink);
												eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onNext(item), signalDeliveryOptions, responseHandler);
											}))
											.publishOn(Schedulers.parallel())
											.subscribe(response -> {}, error -> {
												if (error instanceof ReplyException) {
													var errorMessageCode = ((ReplyException) error).failureCode();
													// -1 == NO_HANDLERS
													if (errorMessageCode == -1) {
														logger.error("Can't send a signal of flux \"" + fluxAddress + "\" because the connection was lost");
													} else {
														logger.error("Error when sending a signal of flux \"" + fluxAddress + "\": {}", error.toString());
													}
												} else {
													logger.error("Error when sending a signal of flux \"" + fluxAddress + "\"", error);
												}
												fatalErrorSink.tryEmitValue(error);
												disposeFlux(atomicSubscription.get(),
														fatalErrorSink,
														subscriptionReady,
														ping,
														cancel,
														dispose,
														fluxAddress,
														() -> {
															logger.warn("Forcefully disposed \"" + fluxAddress + "\" caused by the previous error");
														}
												);
											}, () -> {
												eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onComplete(), signalDeliveryOptions, msg2 -> {
													logger.info("Completed flux \"" + fluxAddress + "\"");
													if (msg2.failed()) {
														logger.error("Failed to send onComplete signal", msg2.cause());
														fatalErrorSink.tryEmitValue(msg2.cause());
													}
												});
											});
									atomicSubscription.set(subscription);

									ping.handler(msg2 -> {
										logger.trace("Client is still alive");
										msg2.reply(EMPTY, deliveryOptions);
									});

									cancel.handler(msg2 -> {
										logger.trace("Cancelling flux \"" + fluxAddress + "\"");
										subscription.dispose();
										logger.debug("Cancelled flux \"" + fluxAddress + "\"");
										msg2.reply(EMPTY, deliveryOptions);
									});

									dispose.handler(msg2 -> {
										disposeFlux(subscription,
												fatalErrorSink,
												subscriptionReady,
												ping,
												cancel,
												dispose,
												fluxAddress,
												() -> msg2.reply(EMPTY)
										);
									});

									ping.completionHandler(h0 -> {
										if (h0.succeeded()) {
											cancel.completionHandler(h1 -> {
												if (h1.succeeded()) {
													dispose.completionHandler(h2 -> {
														if (h2.succeeded()) {
															subscriptionReadyMsg.reply((Long) subscriptionId);
														} else {
															logger.error("Failed to register dispose", h1.cause());
															subscriptionReadyMsg.fail(500, "Failed to register dispose");
														}
													});
												} else {
													logger.error("Failed to register cancel", h1.cause());
													subscriptionReadyMsg.fail(500, "Failed to register cancel");
												}
											});
										} else {
											logger.error("Failed to register ping", h0.cause());
											subscriptionReadyMsg.fail(500, "Failed to register ping");
										}
									});
								} else {
									logger.error("Failed to unregister \"subscription ready\"");
								}
							});
						});

						subscriptionReady.completionHandler(srh -> {
							if (srh.succeeded()) {
								msg.reply((Long) subscriptionId);
							} else {
								logger.error("Failed to register \"subscription ready\"", srh.cause());
								msg.fail(500, "Failed to register \"subscription ready\"");
							}
						});
					} else {
						logger.error("Failed to unregister subscribe consumer");
					}
				});
			});

			subscribe.completionHandler(h -> {
				if (h.failed()) {
					sink.error(h.cause());
				} else {
					sink.success();
				}
			});
		}).publishOn(Schedulers.parallel()).share();

		return Tuples.of(servedMono, fatalErrorSink.asMono());
	}

	private static void disposeFlux(@Nullable Disposable subscription,
			One<Throwable> fatalErrorSink,
			MessageConsumer<byte[]> subscriptionReady,
			MessageConsumer<byte[]> ping,
			MessageConsumer<byte[]> cancel,
			MessageConsumer<byte[]> dispose,
			String fluxAddress,
			Runnable after) {
		logger.trace("Disposing flux \"" + fluxAddress + "\"");
		fatalErrorSink.tryEmitEmpty();
		if (subscription != null) {
			subscription.dispose();
		}
		subscriptionReady.unregister(v0 -> {
			if (v0.failed()) {
				logger.error("Failed to unregister subscriptionReady", v0.cause());
			}
			ping.unregister(v1 -> {
				if (v1.failed()) {
					logger.error("Failed to unregister ping", v1.cause());
				}
				cancel.unregister(v2 -> {
					if (v2.failed()) {
						logger.error("Failed to unregister cancel", v2.cause());
					}
					dispose.unregister(v3 -> {
						if (v3.failed()) {
							logger.error("Failed to unregister dispose", v3.cause());
						}
						logger.debug("Disposed flux \"" + fluxAddress + "\"");
						after.run();
					});
				});
			});
		});
	}

	public static <T> Flux<T> connect(EventBus eventBus,
			String fluxAddress,
			DeliveryOptions baseDeliveryOptions,
			MessageCodec<T, T> itemsCodec,
			Duration connectionTimeout) {
		EventBusFlux.registerFluxCodec(eventBus, itemsCodec);
		return Flux.<T>create(emitter -> {
			var deliveryOptions = new DeliveryOptions(baseDeliveryOptions)
					.setSendTimeout(connectionTimeout.toMillis());
			eventBus.<Long>request(fluxAddress + ".subscribe", EMPTY, deliveryOptions, msg -> {
				if (msg.succeeded()) {
					long subscriptionId = msg.result().body();
					var subscriptionAddress = fluxAddress + "." + subscriptionId;

					var signalConsumer = eventBus.<SignalMessage<T>>consumer(subscriptionAddress + ".signal");
					signalConsumer.handler(msg2 -> {
						var signal = msg2.body();
						switch (signal.getSignalType()) {
							case ITEM:
								emitter.next(signal.getItem());
								break;
							case ERROR:
								emitter.error(new Exception(signal.getErrorMessage()));
								break;
							case COMPLETE:
								emitter.complete();
								break;
						}
						msg2.reply(EMPTY);
					});
					signalConsumer.completionHandler(h -> {
						if (h.succeeded()) {
							eventBus.<Long>request(subscriptionAddress + ".subscriptionReady", EMPTY, deliveryOptions, msg2 -> {
								if (msg2.failed()) {
									logger.error("Failed to tell that the subscription is ready");
								}
							});
						} else {
							emitter.error(new IllegalStateException("Signal consumer registration failed", msg.cause()));
						}
					});

					var pingSubscription = Flux.interval(Duration.ofSeconds(10)).flatMapSequential(n -> Mono.create(pingSink ->
							eventBus.<byte[]>request(subscriptionAddress + ".ping", EMPTY, deliveryOptions, pingMsg -> {
								if (pingMsg.succeeded()) {
									pingSink.success(pingMsg.result().body());
								} else {
									var pingError = pingMsg.cause();
									if (pingError instanceof ReplyException) {
										var pingReplyException = (ReplyException) pingError;
										// -1 = NO_HANDLERS
										if (pingReplyException.failureCode() == -1) {
											pingSink.error(new ConnectException( "Can't send a ping to flux \"" + fluxAddress + "\" because the connection was lost"));
										} else {
											pingSink.error(new ConnectException("Ping failed:" + pingReplyException.toString()));
										}
									} else {
										pingSink.error(new IllegalStateException("Ping failed", pingError));
									}
								}
							})))
							.publishOn(Schedulers.boundedElastic())
							.onBackpressureBuffer()
							.publishOn(Schedulers.parallel())
							.subscribe(v -> {}, emitter::error);

					emitter.onDispose(() -> {
						if (!pingSubscription.isDisposed()) {
							pingSubscription.dispose();
						}
						eventBus.request(subscriptionAddress + ".dispose", EMPTY, deliveryOptions, msg2 -> {
							if (msg.failed()) {
								logger.error("Failed to tell that the subscription is disposed");
							}
						});
					});

					emitter.onCancel(() -> {
						if (!pingSubscription.isDisposed()) {
							pingSubscription.dispose();
						}
						eventBus.request(subscriptionAddress + ".cancel", EMPTY, deliveryOptions, msg2 -> {
							if (msg.failed()) {
								logger.error("Failed to tell that the subscription is cancelled");
							}
						});
					});
				} else {
					emitter.error(new IllegalStateException("Subscription failed", msg.cause()));
				}
			});
		});
	}

}
