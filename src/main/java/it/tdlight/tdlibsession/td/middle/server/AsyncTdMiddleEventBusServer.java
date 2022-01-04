package it.tdlight.tdlibsession.td.middle.server;

import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import io.vertx.reactivex.core.eventbus.MessageProducer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.remoteclient.TDLibRemoteClient;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.direct.TelegramClientFactory;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.tdlibsession.td.middle.TdResultMessage;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import java.net.ConnectException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class AsyncTdMiddleEventBusServer extends AbstractVerticle {

	// Static values
	protected static final Logger logger = LogManager.getLogger("TdMiddleServer");
	public static final byte[] EMPTY = new byte[0];
	public static final Duration WAIT_DURATION = Duration.ofSeconds(1);

	// Values configured from constructor
	private final AsyncTdDirectOptions tdOptions;
	private final TelegramClientFactory clientFactory;

	// Variables configured by the user at startup
	private final AtomicReference<Integer> botId = new AtomicReference<>();
	private final AtomicReference<String> botAddress = new AtomicReference<>();
	private final AtomicReference<String> botAlias = new AtomicReference<>();

	// Variables configured at startup
	private final AtomicReference<AsyncTdDirectImpl> td = new AtomicReference<>();
	private final AtomicReference<MessageConsumer<ExecuteObject<?>>> executeConsumer = new AtomicReference<>();
	private final AtomicReference<MessageConsumer<byte[]>> readBinlogConsumer = new AtomicReference<>();
	private final AtomicReference<MessageConsumer<byte[]>> readyToReceiveConsumer = new AtomicReference<>();
	private final AtomicReference<MessageConsumer<byte[]>> pingConsumer = new AtomicReference<>();
	private final AtomicReference<Flux<Void>> pipeFlux = new AtomicReference<>();

	public AsyncTdMiddleEventBusServer() {
		this.tdOptions = new AsyncTdDirectOptions(WAIT_DURATION, 100);
		this.clientFactory = new TelegramClientFactory();
	}

	@Override
	public Completable rxStart() {
		return MonoUtils
				.toCompletable(MonoUtils
						.fromBlockingMaybe(() -> {
							logger.trace("Stating verticle");
							var botId = config().getInteger("botId");
							if (botId == null || botId <= 0) {
								throw new IllegalArgumentException("botId is not set!");
							}
							this.botId.set(botId);
							var botAddress = "bots.bot." + botId;
							this.botAddress.set(botAddress);
							var botAlias = config().getString("botAlias");
							if (botAlias == null || botAlias.isEmpty()) {
								throw new IllegalArgumentException("botAlias is not set!");
							}
							this.botAlias.set(botAlias);
							var local = config().getBoolean("local");
							if (local == null) {
								throw new IllegalArgumentException("local is not set!");
							}
							var implementationDetails = config().getJsonObject("implementationDetails");
							if (implementationDetails == null) {
								throw new IllegalArgumentException("implementationDetails is not set!");
							}

							var td = new AsyncTdDirectImpl(clientFactory, implementationDetails, botAlias);
							this.td.set(td);
							return new OnSuccessfulStartRequestInfo(td, botAddress, botAlias, botId, local);
						})
						.flatMap(r -> onSuccessfulStartRequest(r.td, r.botAddress, r.botAlias, r.botId, r.local))
						.doOnSuccess(s -> logger.trace("Stated verticle"))
				);
	}

	private static class OnSuccessfulStartRequestInfo {

		public final AsyncTdDirectImpl td;
		public final String botAddress;
		public final String botAlias;
		public final int botId;
		public final boolean local;

		public OnSuccessfulStartRequestInfo(AsyncTdDirectImpl td, String botAddress, String botAlias, int botId,
				boolean local) {
			this.td = td;
			this.botAddress = botAddress;
			this.botAlias = botAlias;
			this.botId = botId;
			this.local = local;
		}
	}

	private Mono<Void> onSuccessfulStartRequest(AsyncTdDirectImpl td, String botAddress, String botAlias, int botId,
			boolean local) {
		return td
				.initialize()
				.then(this.pipe(td, botAddress, local))
				.then(this.listen(td, botAddress, botId, local))
				.doOnSuccess(s -> logger.info("Deploy and start of bot \"" + botAlias + "\": ✅ Succeeded"))
				.doOnError(ex -> logger.error("Deploy and start of bot \"" + botAlias + "\": ❌ Failed", ex));
	}

	private Mono<Void> listen(AsyncTdDirectImpl td, String botAddress, int botId, boolean local) {
		return Mono.<Void>create(registrationSink -> {
			logger.trace("Preparing listeners");

			MessageConsumer<ExecuteObject<?>> executeConsumer = vertx.eventBus().consumer(botAddress + ".execute");
			this.executeConsumer.set(executeConsumer);
			Flux
					.<Message<ExecuteObject<?>>>create(sink -> {
						executeConsumer.handler(sink::next);
						executeConsumer.endHandler(h -> sink.complete());
					})
					.flatMap(msg -> {
						var body = msg.body();
						var request = overrideRequest(body.getRequest(), botId);
						var timeout = body.getTimeout();
						if (logger.isTraceEnabled()) {
							logger.trace("Received execute request {}", request);
						}
						return td
								.execute(request, timeout, body.isExecuteDirectly())
								.single()
								.doOnSuccess(s -> logger.trace("Executed successfully. Request was {}", request))
								.onErrorResume(ex -> Mono.fromRunnable(() -> msg.fail(500, ex.getLocalizedMessage())))
								.flatMap(response -> Mono.fromCallable(() -> {
									var replyOpts = new DeliveryOptions().setLocalOnly(local);
									var replyValue = new TdResultMessage(response.result(), response.cause());
									try {
										logger.trace("Replying with success response. Request was {}", request);
										msg.reply(replyValue, replyOpts);
										return response;
									} catch (Exception ex) {
										logger.debug("Replying with error response: {}. Request was {}", ex.getLocalizedMessage(),
												request);
										msg.fail(500, ex.getLocalizedMessage());
										throw ex;
									}
								}).subscribeOn(Schedulers.boundedElastic()));
					})
					.then()
					.subscribeOn(Schedulers.parallel())
					.subscribe(v -> {},
							ex -> logger.error("Fatal error when processing an execute request."
									+ " Can't process further requests since the subscription has been broken", ex),
							() -> logger.trace("Finished handling execute requests")
					);

			MessageConsumer<byte[]> readBinlogConsumer = vertx.eventBus().consumer(botAddress + ".read-binlog");
			this.readBinlogConsumer.set(readBinlogConsumer);
			BinlogUtils
					.readBinlogConsumer(vertx, readBinlogConsumer, botId, local)
					.subscribeOn(Schedulers.parallel())
					.subscribe(v -> {}, ex -> logger.error("Error when processing a read-binlog request", ex));

			MessageConsumer<byte[]> readyToReceiveConsumer = vertx.eventBus().consumer(botAddress
					+ ".ready-to-receive");
			this.readyToReceiveConsumer.set(readyToReceiveConsumer);

			// Pipe the data
			Flux
					.<Message<byte[]>>create(sink -> {
						readyToReceiveConsumer.handler(sink::next);
						readyToReceiveConsumer.endHandler(h -> sink.complete());
					})
					.take(1, true)
					.single()
					.doOnNext(s -> logger.trace("Received ready-to-receive request from client"))
					.map(msg -> Tuples.of(msg, Objects.requireNonNull(pipeFlux.get(), "PipeFlux is empty")))
					.doOnError(ex -> logger.error("Error when processing a ready-to-receive request", ex))
					.doOnNext(s -> logger.trace("Replying to ready-to-receive request"))
					.flatMapMany(tuple -> {
						var opts = new DeliveryOptions().setLocalOnly(local)
								.setSendTimeout(Duration.ofSeconds(10).toMillis());

						tuple.getT1().reply(EMPTY, opts);
						logger.trace("Replied to ready-to-receive");

						logger.trace("Start piping data");

						// Start piping the data
						return tuple.getT2().doOnSubscribe(s -> logger.trace("Subscribed to updates pipe"));
					})
					.then()
					.doOnSuccess(s -> logger.trace("Finished handling ready-to-receive requests (updates pipe ended)"))
					.subscribeOn(Schedulers.boundedElastic())
					// Don't handle errors here. Handle them in pipeFlux
					.subscribe(v -> {});

			MessageConsumer<byte[]> pingConsumer = vertx.eventBus().consumer(botAddress + ".ping");
			this.pingConsumer.set(pingConsumer);
			Flux
					.<Message<byte[]>>create(sink -> {
						pingConsumer.handler(sink::next);
						pingConsumer.endHandler(h -> sink.complete());
					})
					.concatMap(msg -> Mono.fromCallable(() -> {
						var opts = new DeliveryOptions().setLocalOnly(local)
								.setSendTimeout(Duration.ofSeconds(10).toMillis());
						msg.reply(EMPTY, opts);
						return null;
					}))
					.then()
					.subscribeOn(Schedulers.boundedElastic())
					.subscribe(v -> {},
							ex -> logger.error("Error when processing a ping request", ex),
							() -> logger.trace("Finished handling ping requests")
					);

			executeConsumer
					.rxCompletionHandler()
					.andThen(readBinlogConsumer.rxCompletionHandler())
					.andThen(readyToReceiveConsumer.rxCompletionHandler())
					.andThen(pingConsumer.rxCompletionHandler())
					.as(MonoUtils::toMono)
					.doOnSuccess(s -> logger.trace("Finished preparing listeners"))
					.subscribeOn(Schedulers.parallel())
					.subscribe(v -> {}, registrationSink::error, registrationSink::success);
		})
				.subscribeOn(Schedulers.boundedElastic());
	}

	/**
	 * Override some requests
	 */
	private <T extends TdApi.Object> Function<T> overrideRequest(Function<T> request, int botId) {
		if (request.getConstructor() == SetTdlibParameters.CONSTRUCTOR) {
			// Fix session directory locations
			var setTdlibParamsObj = (SetTdlibParameters) request;
			setTdlibParamsObj.parameters.databaseDirectory = TDLibRemoteClient.getSessionDirectory(botId).toString();
			setTdlibParamsObj.parameters.filesDirectory = TDLibRemoteClient.getMediaDirectory(botId).toString();
		}
		return request;
	}

	@Override
	public Completable rxStop() {
		return MonoUtils.toCompletable(Mono
				.fromRunnable(() -> logger.info("Undeploy of bot \"" + botAlias.get() + "\": stopping"))
				.then(Mono
						.fromCallable(executeConsumer::get)
						.flatMap(executeConsumer -> executeConsumer.rxUnregister().as(MonoUtils::toMono))
						.doOnSuccess(s -> logger.trace("Unregistered execute consumer"))
				)
				.then(MonoUtils.fromBlockingEmpty(() -> {
					var readBinlogConsumer = this.readBinlogConsumer.get();
					if (readBinlogConsumer != null) {
						Mono
								// ReadBinLog will live for another 10 minutes.
								// Since every consumer of ReadBinLog is identical, this should not pose a problem.
								.delay(Duration.ofMinutes(10))
								.then(readBinlogConsumer.rxUnregister().as(MonoUtils::toMono))
								.subscribe();
					}
				}))
				.then(Mono
						.fromCallable(readyToReceiveConsumer::get)
						.flatMap(ec -> ec.rxUnregister().as(MonoUtils::toMono))
				)
				.then(Mono
						.fromCallable(pingConsumer::get)
						.flatMap(ec -> ec.rxUnregister().as(MonoUtils::toMono))
				)
				.doOnError(ex -> logger.error("Undeploy of bot \"" + botAlias.get() + "\": stop failed", ex))
				.doOnTerminate(() -> logger.info("Undeploy of bot \"" + botAlias.get() + "\": stopped"))
		);
	}

	private Mono<Void> pipe(AsyncTdDirectImpl td, String botAddress, boolean local) {
		logger.trace("Preparing to pipe requests");
		Flux<TdResultList> updatesFlux = td
				.receive(tdOptions)
				.takeUntil(item -> {
					if (item instanceof Update) {
						var tdUpdate = (Update) item;
						if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
							var updateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
							return updateAuthorizationState.authorizationState.getConstructor()
									== AuthorizationStateClosed.CONSTRUCTOR;
						}
					} else
						return item instanceof Error;
					return false;
				})
				.flatMap(update -> Mono.fromCallable(() -> {
					if (update.getConstructor() == TdApi.Error.CONSTRUCTOR) {
						var error = (Error) update;
						throw new TdError(error.code, error.message);
					} else {
						return update;
					}
				}))
				.limitRate(Math.max(1, tdOptions.getEventsSize()))
				//.transform(normal -> new BufferTimeOutPublisher<>(normal,Math.max(1, tdOptions.getEventsSize()),
				//		local ? Duration.ofMillis(1) : Duration.ofMillis(100), false))
				//.bufferTimeout(Math.max(1, tdOptions.getEventsSize()), local ? Duration.ofMillis(1) : Duration.ofMillis(100))
				.map(List::of)
				.limitRate(Math.max(1, tdOptions.getEventsSize()))
				.map(TdResultList::new);

		var fluxCodec = new TdResultListMessageCodec();
		var opts = new DeliveryOptions()
				.setLocalOnly(local)
				.setSendTimeout(Duration.ofSeconds(30).toMillis())
				.setCodecName(fluxCodec.name());

		MessageProducer<TdResultList> updatesSender = vertx
				.eventBus()
				.sender(botAddress + ".updates", opts);

		var pipeFlux = updatesFlux
				.concatMap(updatesList -> updatesSender
						.rxWrite(updatesList)
						.as(MonoUtils::toMono)
						.thenReturn(updatesList)
				)
				.concatMap(updatesList -> Flux
						.fromIterable(updatesList.value())
						.concatMap(item -> {
							if (item instanceof Update) {
								var tdUpdate = (Update) item;
								if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
									var tdUpdateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
									if (tdUpdateAuthorizationState.authorizationState.getConstructor()
											== AuthorizationStateClosed.CONSTRUCTOR) {
										logger.info("Undeploying after receiving AuthorizationStateClosed");
										return rxStop().as(MonoUtils::toMono).thenReturn(item);
									}
								}
							} else if (item instanceof Error) {
								// An error in updates means that a fatal error occurred
								logger.info("Undeploying after receiving a fatal error");
								return rxStop().as(MonoUtils::toMono).thenReturn(item);
							}
							return Mono.just(item);
						})
						.then()
				)
				.doOnTerminate(() -> updatesSender.close(h -> {
					if (h.failed()) {
						logger.error("Failed to close \"updates\" message sender");
					}
				}))
				.onErrorResume(ex -> {
					boolean printDefaultException = true;
					if (ex instanceof ReplyException) {
						ReplyException replyException = (ReplyException) ex;
						if (replyException.failureCode() == -1 && replyException.failureType() == ReplyFailure.NO_HANDLERS) {
							logger.warn("Undeploying, the flux has been terminated because no more"
									+ " handlers are available on the event bus. {}", replyException.getMessage());
							printDefaultException = false;
						}
					} else if (ex instanceof ConnectException || ex instanceof java.nio.channels.ClosedChannelException) {
						logger.warn("Undeploying, the flux has been terminated because the consumer"
								+ " disconnected from the event bus. {}", ex.getMessage());
						printDefaultException = false;
					}
					if (printDefaultException) {
						logger.warn("Undeploying after a fatal error in a served flux", ex);
					}
					return td
							.execute(new TdApi.Close(), Duration.ofDays(1), false)
							.doOnError(ex2 -> logger.error("Unexpected error", ex2))
							.doOnSuccess(s -> logger.debug("Emergency Close() signal has been sent successfully"))
							.then(rxStop().as(MonoUtils::toMono));
				});

		return MonoUtils.fromBlockingEmpty(() -> {
			this.pipeFlux.set(pipeFlux);
			logger.trace("Prepared piping requests successfully");
		});
	}
}
