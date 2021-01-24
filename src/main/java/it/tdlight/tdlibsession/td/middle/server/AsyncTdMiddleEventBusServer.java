package it.tdlight.tdlibsession.td.middle.server;

import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
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
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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

	public AsyncTdMiddleEventBusServer() {
		this.tdOptions = new AsyncTdDirectOptions(WAIT_DURATION, 100);
	}

	@Override
	public Completable rxStart() {
		return MonoUtils.toCompletable(Mono
				.fromCallable(() -> {
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
					if (this.local.tryEmitValue(local).isFailure()) {
						throw new IllegalStateException("Failed to set local");
					}

					var td = new AsyncTdDirectImpl(botAlias);
					if (this.td.tryEmitValue(td).isFailure()) {
						throw new IllegalStateException("Failed to set td instance");
					}
					return onSuccessfulStartRequest(td, botAddress, botAlias, botId, local);
				})
				.flatMap(Mono::hide));
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
						var request = overrideRequest(msg.body().getRequest(), botId);
						return td
								.execute(request, msg.body().isExecuteDirectly())
								.map(result -> Tuples.of(msg, result));
					})
					.handle((tuple, sink) -> {
						var msg = tuple.getT1();
						var response = tuple.getT2();
						var replyOpts = new DeliveryOptions().setLocalOnly(local);
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
					.subscribeOn(Schedulers.single())
					.subscribe(v -> {}, ex -> logger.error("Error when processing an execute request", ex));

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
			Flux
					.<Message<byte[]>>create(sink -> {
						readyToReceiveConsumer.handler(sink::next);
						readyToReceiveConsumer.endHandler(h -> sink.complete());
					})
					.flatMap(msg -> this.pipeFlux
							.asMono()
							.timeout(Duration.ofSeconds(5))
							.map(pipeFlux -> Tuples.of(msg, pipeFlux)))
					.doOnNext(tuple -> {
						var opts = new DeliveryOptions().setLocalOnly(local).setSendTimeout(Duration.ofSeconds(10).toMillis());
						tuple.getT1().reply(EMPTY, opts);

						// Start piping the data
						//noinspection CallingSubscribeInNonBlockingScope
						tuple.getT2()
								.subscribeOn(Schedulers.single())
								.subscribe();
					})
					.then()
					.subscribeOn(Schedulers.single())
					.subscribe(v -> {}, ex -> logger.error("Error when processing a ready-to-receive request", ex));

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
					.subscribe(v -> {}, ex -> logger.error("Error when processing a ping request", ex));


			//noinspection ResultOfMethodCallIgnored
			executeConsumer
					.rxCompletionHandler()
					.andThen(readBinlogConsumer.rxCompletionHandler())
					.andThen(readyToReceiveConsumer.rxCompletionHandler())
					.andThen(pingConsumer.rxCompletionHandler())
					.subscribeOn(io.reactivex.schedulers.Schedulers.single())
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
		Flux<TdResultList> updatesFlux = td
				.receive(tdOptions)
				.flatMap(item -> Mono.defer(() -> {
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
				}))
				.flatMap(item -> Mono.fromCallable(() -> {
					if (item.getConstructor() == TdApi.Error.CONSTRUCTOR) {
						var error = (Error) item;
						throw new TdError(error.code, error.message);
					} else {
						return item;
					}
				}))
				.bufferTimeout(tdOptions.getEventsSize(), local ? Duration.ofMillis(1) : Duration.ofMillis(100))
				.windowTimeout(1, Duration.ofSeconds(5))
				.flatMap(w -> w.defaultIfEmpty(Collections.emptyList()))
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
				.flatMap(update -> updatesSender.rxWrite(update).as(MonoUtils::toMono).then())
				.doOnTerminate(() -> updatesSender.close(h -> {
					if (h.failed()) {
						logger.error("Failed to close \"updates\" message sender");
					}
				}))
				.onErrorResume(ex -> {
					logger.warn("Undeploying after a fatal error in a served flux", ex);
					return td.execute(new TdApi.Close(), false)
							.doOnError(ex2 -> logger.error("Unexpected error", ex2))
							.then();
				});
		MonoUtils.emitValue(this.pipeFlux, pipeFlux);
		return Mono.empty();
	}
}
