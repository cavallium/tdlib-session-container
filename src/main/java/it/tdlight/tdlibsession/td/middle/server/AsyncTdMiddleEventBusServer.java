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
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.direct.TelegramClientFactory;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class AsyncTdMiddleEventBusServer extends AbstractVerticle {

	// Static values
	protected static final Logger logger = LoggerFactory.getLogger("TdMiddleServer");
	public static final byte[] EMPTY = new byte[0];
	public static final Duration WAIT_DURATION = Duration.ofSeconds(1);

	// Values configured from constructor
	private final AsyncTdDirectOptions tdOptions;
	private final TelegramClientFactory clientFactory;

	// Variables configured by the user at startup
	private final One<Integer> botId = Sinks.one();
	private final One<String> botAddress = Sinks.one();
	private final One<String> botAlias = Sinks.one();
	private final One<Boolean> local = Sinks.one();

	// Variables configured at startup
	private final One<AsyncTdDirectImpl> td = Sinks.one();
	private final One<MessageConsumer<ExecuteObject>> executeConsumer = Sinks.one();
	private final One<MessageConsumer<byte[]>> readBinlogConsumer = Sinks.one();
	private final One<MessageConsumer<byte[]>> readyToReceiveConsumer = Sinks.one();
	private final One<MessageConsumer<byte[]>> pingConsumer = Sinks.one();
	private final One<Flux<Void>> pipeFlux = Sinks.one();
	private final Empty<Void> terminatePingOverPipeFlux = Sinks.empty();

	public AsyncTdMiddleEventBusServer() {
		this.tdOptions = new AsyncTdDirectOptions(WAIT_DURATION, 50);
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
							if (this.botId.tryEmitValue(botId).isFailure()) {
								throw new IllegalStateException("Failed to set botId");
							}
							var botAddress = "bots.bot." + botId;
							if (this.botAddress.tryEmitValue(botAddress).isFailure()) {
								throw new IllegalStateException("Failed to set botAddress");
							}
							var botAlias = config().getString("botAlias");
							if (botAlias == null || botAlias.isEmpty()) {
								throw new IllegalArgumentException("botAlias is not set!");
							}
							if (this.botAlias.tryEmitValue(botAlias).isFailure()) {
								throw new IllegalStateException("Failed to set botAlias");
							}
							var local = config().getBoolean("local");
							if (local == null) {
								throw new IllegalArgumentException("local is not set!");
							}
							var implementationDetails = config().getJsonObject("implementationDetails");
							if (implementationDetails == null) {
								throw new IllegalArgumentException("implementationDetails is not set!");
							}

							var td = new AsyncTdDirectImpl(clientFactory, implementationDetails, botAlias);
							if (this.td.tryEmitValue(td).isFailure()) {
								throw new IllegalStateException("Failed to set td instance");
							}
							return onSuccessfulStartRequest(td, botAddress, botAlias, botId, local);
						})
						.flatMap(Mono::hide)
						.doOnSuccess(s -> logger.trace("Stated verticle"))
				);
	}

	private Mono<Void> onSuccessfulStartRequest(AsyncTdDirectImpl td, String botAddress, String botAlias, int botId, boolean local) {
		return this
				.listen(td, botAddress, botAlias, botId, local)
				.then(this.pipe(td, botAddress, botAlias, botId, local))
				.doOnSuccess(s -> {
					logger.info("Deploy and start of bot \"" + botAlias + "\": ✅ Succeeded");
				})
				.doOnError(ex -> {
					logger.error("Deploy and start of bot \"" + botAlias + "\": ❌ Failed", ex);
				});
	}

	private Mono<Void> listen(AsyncTdDirectImpl td, String botAddress, String botAlias, int botId, boolean local) {
		return Mono.<Void>create(registrationSink -> {
			logger.trace("Preparing listeners");

			MessageConsumer<ExecuteObject> executeConsumer = vertx.eventBus().consumer(botAddress + ".execute");
			if (this.executeConsumer.tryEmitValue(executeConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set executeConsumer"));
				return;
			}
			Flux
					.<Message<ExecuteObject>>create(sink -> {
						executeConsumer.handler(sink::next);
						executeConsumer.endHandler(h -> sink.complete());
					})
					.flatMap(msg -> {
						logger.trace("Received execute request {}", msg.body());
						var request = overrideRequest(msg.body().getRequest(), botId);
						return td
								.execute(request, msg.body().isExecuteDirectly())
								.map(result -> Tuples.of(msg, result))
								.doOnSuccess(s -> logger.trace("Executed successfully"));
					})
					.handle((tuple, sink) -> {
						var msg = tuple.getT1();
						var response = tuple.getT2();
						var replyOpts = new DeliveryOptions().setLocalOnly(local);
						var replyValue = new TdResultMessage(response.result(), response.cause());
						try {
							logger.trace("Replying with success response");
							msg.reply(replyValue, replyOpts);
							sink.next(response);
						} catch (Exception ex) {
							logger.trace("Replying with error response: {}", ex.getLocalizedMessage());
							msg.fail(500, ex.getLocalizedMessage());
							sink.error(ex);
						}
					})
					.then()
					.subscribeOn(Schedulers.single())
					.subscribe(v -> {},
							ex -> logger.error("Error when processing an execute request", ex),
							() -> logger.trace("Finished handling execute requests")
					);

			MessageConsumer<byte[]> readBinlogConsumer = vertx.eventBus().consumer(botAddress + ".read-binlog");
			if (this.readBinlogConsumer.tryEmitValue(readBinlogConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set readBinlogConsumer"));
				return;
			}
			BinlogUtils
					.readBinlogConsumer(vertx, readBinlogConsumer, botId, local)
					.subscribeOn(Schedulers.single())
					.subscribe(v -> {}, ex -> logger.error("Error when processing a read-binlog request", ex));

			MessageConsumer<byte[]> readyToReceiveConsumer = vertx.eventBus().consumer(botAddress + ".ready-to-receive");
			if (this.readyToReceiveConsumer.tryEmitValue(readyToReceiveConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set readyToReceiveConsumer"));
				return;
			}

			// Pipe  the data
			var pipeSubscription = Flux
					.<Message<byte[]>>create(sink -> {
						readyToReceiveConsumer.handler(sink::next);
						readyToReceiveConsumer.endHandler(h -> sink.complete());
					})
					.take(1)
					.limitRequest(1)
					.single()
					.flatMap(msg -> this.pipeFlux
							.asMono()
							.timeout(Duration.ofSeconds(5))
							.map(pipeFlux -> Tuples.of(msg, pipeFlux)))
					.doOnError(ex -> logger.error("Error when processing a ready-to-receive request", ex))
					.flatMapMany(tuple -> {
						var opts = new DeliveryOptions().setLocalOnly(local).setSendTimeout(Duration.ofSeconds(10).toMillis());

						tuple.getT1().reply(EMPTY, opts);
						logger.trace("Replied to ready-to-receive");

						// Start piping the data
						return tuple.getT2().doOnSubscribe(s -> {
							logger.trace("Subscribed to updates pipe");
						});
					})
					.then()
					.doOnSuccess(s -> logger.trace("Finished handling ready-to-receive requests (updates pipe ended)"))
					.subscribeOn(Schedulers.single())
					// Don't handle errors here. Handle them in pipeFlux
					.subscribe(v -> {});

			MessageConsumer<byte[]> pingConsumer = vertx.eventBus().consumer(botAddress + ".ping");
			if (this.pingConsumer.tryEmitValue(pingConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set pingConsumer"));
				return;
			}
			Flux
					.<Message<byte[]>>create(sink -> {
						pingConsumer.handler(sink::next);
						pingConsumer.endHandler(h -> sink.complete());
					})
					.doOnNext(msg -> {
						var opts = new DeliveryOptions().setLocalOnly(local).setSendTimeout(Duration.ofSeconds(10).toMillis());
						msg.reply(EMPTY, opts);
					})
					.then()
					.subscribeOn(Schedulers.single())
					.subscribe(v -> {},
							ex -> logger.error("Error when processing a ping request", ex),
							() -> logger.trace("Finished handling ping requests")
					);


			//noinspection ResultOfMethodCallIgnored
			executeConsumer
					.rxCompletionHandler()
					.andThen(readBinlogConsumer.rxCompletionHandler())
					.andThen(readyToReceiveConsumer.rxCompletionHandler())
					.andThen(pingConsumer.rxCompletionHandler())
					.subscribeOn(io.reactivex.schedulers.Schedulers.single())
					.doOnComplete(() -> logger.trace("Finished preparing listeners"))
					.subscribe(registrationSink::success, registrationSink::error);
		});
	}

	/**
	 * Override some requests
	 */
	private Function overrideRequest(Function request, int botId) {
		switch (request.getConstructor()) {
			case SetTdlibParameters.CONSTRUCTOR:
				// Fix session directory locations
				var setTdlibParamsObj = (SetTdlibParameters) request;
				setTdlibParamsObj.parameters.databaseDirectory = TDLibRemoteClient.getSessionDirectory(botId).toString();
				setTdlibParamsObj.parameters.filesDirectory = TDLibRemoteClient.getMediaDirectory(botId).toString();
				return request;
			default:
				return request;
		}
	}

	@Override
	public Completable rxStop() {
		return MonoUtils.toCompletable(botAlias
				.asMono()
				.timeout(Duration.ofSeconds(1), Mono.just("???"))
				.flatMap(botAlias -> Mono
						.fromRunnable(() -> logger.info("Undeploy of bot \"" + botAlias + "\": stopping"))
						.then(executeConsumer
								.asMono()
								.timeout(Duration.ofSeconds(5), Mono.empty())
								.flatMap(ec -> ec.rxUnregister().as(MonoUtils::toMono)))
						.then(readBinlogConsumer
								.asMono()
								.timeout(Duration.ofSeconds(10), Mono.empty())
								.doOnNext(ec -> Mono
										// ReadBinLog will live for another 30 minutes.
										// Since every consumer of ReadBinLog is identical, this should not pose a problem.
										.delay(Duration.ofMinutes(30))
										.then(ec.rxUnregister().as(MonoUtils::toMono))
										.subscribeOn(Schedulers.single())
										.subscribe()
								)
						)
						.then(readyToReceiveConsumer
								.asMono()
								.timeout(Duration.ofSeconds(5), Mono.empty())
								.flatMap(ec -> ec.rxUnregister().as(MonoUtils::toMono)))
						.then(pingConsumer
								.asMono()
								.timeout(Duration.ofSeconds(5), Mono.empty())
								.flatMap(ec -> ec.rxUnregister().as(MonoUtils::toMono)))
						.doOnError(ex -> logger.error("Undeploy of bot \"" + botAlias + "\": stop failed", ex))
						.doOnTerminate(() -> logger.info("Undeploy of bot \"" + botAlias + "\": stopped"))));
	}

	private Mono<Void> pipe(AsyncTdDirectImpl td, String botAddress, String botAlias, int botId, boolean local) {
		logger.trace("Preparing to pipe requests");
		Flux<TdResultList> updatesFlux = td
				.receive(tdOptions)
				.takeUntil(item -> {
					if (item instanceof Update) {
						var tdUpdate = (Update) item;
						if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
							var updateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
							if (updateAuthorizationState.authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR) {
								return true;
							}
						}
					} else if (item instanceof Error) {
						return true;
					}
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
				.bufferTimeout(tdOptions.getEventsSize(), local ? Duration.ofMillis(1) : Duration.ofMillis(100))
				.doFinally(signalType -> terminatePingOverPipeFlux.tryEmitEmpty())
				.mergeWith(Flux
						.interval(Duration.ofSeconds(5))
						.map(l -> Collections.<TdApi.Object>emptyList())
						.takeUntilOther(terminatePingOverPipeFlux.asMono()))
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
				.flatMap(updatesList -> updatesSender
						.rxWrite(updatesList)
						.as(MonoUtils::toMono)
						.thenReturn(updatesList)
				)
				.flatMap(updatesList -> Flux
						.fromIterable(updatesList.value())
						.flatMap(item -> {
							if (item instanceof Update) {
								var tdUpdate = (Update) item;
								if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
									var tdUpdateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
									if (tdUpdateAuthorizationState.authorizationState.getConstructor()
											== AuthorizationStateClosed.CONSTRUCTOR) {
										logger.debug("Undeploying after receiving AuthorizationStateClosed");
										return rxStop().as(MonoUtils::toMono).thenReturn(item);
									}
								}
							} else if (item instanceof Error) {
								// An error in updates means that a fatal error occurred
								logger.debug("Undeploying after receiving a fatal error");
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
							logger.warn("Undeploying, the flux has been terminated because no more handlers are available on the event bus. {}", replyException.getMessage());
							printDefaultException = false;
						}
					} else if (ex instanceof ConnectException || ex instanceof java.nio.channels.ClosedChannelException) {
						logger.warn("Undeploying, the flux has been terminated because the consumer disconnected from the event bus. {}", ex.getMessage());
						printDefaultException = false;
					}
					if (printDefaultException) {
						logger.warn("Undeploying after a fatal error in a served flux", ex);
					}
					return td.execute(new TdApi.Close(), false)
							.doOnError(ex2 -> logger.error("Unexpected error", ex2))
							.doOnSuccess(s -> logger.debug("Emergency Close() signal has been sent successfully"))
							.then(rxStop().as(MonoUtils::toMono));
				});

		return MonoUtils.emitValue(this.pipeFlux, pipeFlux)
				.doOnSuccess(s -> logger.trace("Prepared piping requests successfully"));
	}
}
