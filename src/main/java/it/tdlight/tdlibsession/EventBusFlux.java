package it.tdlight.tdlibsession;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.MessageConsumer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

	public static <T> Mono<Void> serve(Flux<T> flux,
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
		return Mono.create(sink -> {
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

						MessageConsumer<byte[]> subscriptionReady = eventBus.consumer(fluxAddress + ".subscriptionReady");
						MessageConsumer<byte[]> dispose = eventBus.consumer(subscriptionAddress + ".dispose");
						MessageConsumer<byte[]> cancel = eventBus.consumer(subscriptionAddress + ".cancel");

						subscriptionReady.<Long>handler(subscriptionReadyMsg -> {
							subscriptionReady.unregister(subscriptionReadyUnregistered -> {
								if (subscriptionReadyUnregistered.succeeded()) {
									var subscription = flux.subscribe(item -> {
										var request = eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onNext(item), signalDeliveryOptions, msg2 -> {
											if (msg2.failed()) {
												logger.error("Failed to send onNext signal", msg2.cause());
											}
										});
									}, error -> {
										eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onError(error), signalDeliveryOptions, msg2 -> {
											if (msg2.failed()) {
												logger.error("Failed to send onNext signal", msg2.cause());
											}
										});
									}, () -> {
										eventBus.request(subscriptionAddress + ".signal", SignalMessage.<T>onComplete(), signalDeliveryOptions, msg2 -> {
											if (msg2.failed()) {
												logger.error("Failed to send onNext signal", msg2.cause());
											}
										});
									});

									cancel.handler(msg3 -> {
										if (!subscription.isDisposed()) {
											subscription.dispose();
										}
										msg3.reply(EMPTY, deliveryOptions);
									});
									dispose.handler(msg2 -> {
										if (!subscription.isDisposed()) {
											subscription.dispose();
										}
										cancel.unregister(v -> {
											if (v.failed()) {
												logger.error("Failed to unregister cancel", v.cause());
											}
											dispose.unregister(v2 -> {
												if (v.failed()) {
													logger.error("Failed to unregister dispose", v2.cause());
												}
												msg2.reply(EMPTY);
											});
										});
									});

									cancel.completionHandler(h -> {
										if (h.succeeded()) {
											dispose.completionHandler(h2 -> {
												if (h2.succeeded()) {
													subscriptionReadyMsg.reply((Long) subscriptionId);
												} else {
													logger.error("Failed to register dispose", h.cause());
													subscriptionReadyMsg.fail(500, "Failed to register dispose");
												}
											});
										} else {
											logger.error("Failed to register cancel", h.cause());
											subscriptionReadyMsg.fail(500, "Failed to register cancel");
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
						if (h.failed()) {
							emitter.error(new IllegalStateException("Signal consumer registration failed", msg.cause()));
						} else {
							eventBus.<Long>request(fluxAddress + ".subscriptionReady", EMPTY, deliveryOptions, msg2 -> {
								if (msg2.failed()) {
									logger.error("Failed to tell that the subscription is ready");
								}
							});
						}
					});

					emitter.onDispose(() -> eventBus.send(subscriptionAddress + ".dispose", EMPTY, deliveryOptions));

					emitter.onCancel(() -> eventBus.send(subscriptionAddress + ".cancel", EMPTY, deliveryOptions));
				} else {
					emitter.error(new IllegalStateException("Subscription failed", msg.cause()));
				}
			});
		});
	}

}
