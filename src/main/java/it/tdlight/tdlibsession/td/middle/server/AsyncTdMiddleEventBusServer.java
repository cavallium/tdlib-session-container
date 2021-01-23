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
import it.tdlight.jni.TdApi.Update;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectImpl;
import it.tdlight.tdlibsession.td.direct.AsyncTdDirectOptions;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.tdlibsession.td.middle.TdResultListMessageCodec;
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
					return onSuccessfulStartRequest(td, botAddress, botAlias, local);
				})
				.flatMap(Mono::hide));
	}

	private Mono<Void> onSuccessfulStartRequest(AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		return this
				.listen(td, botAddress, botAlias, local)
				.then(this.pipe(td, botAddress, botAlias, local))
				.doOnSuccess(s -> {
					logger.info("Deploy and start of bot \"" + botAlias + "\": ✅ Succeeded");
				})
				.doOnError(ex -> {
					logger.error("Deploy and start of bot \"" + botAlias + "\": ❌ Failed", ex);
				});
	}

	private Mono<Void> listen(AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		return Mono.<Void>create(registrationSink -> {
			MessageConsumer<ExecuteObject> executeConsumer = vertx.eventBus().consumer(botAddress + ".execute");
			if (this.executeConsumer.tryEmitValue(executeConsumer).isFailure()) {
				registrationSink.error(new IllegalStateException("Failed to set executeConsumer"));
				return;
			}

			Flux.<Message<ExecuteObject>>create(sink -> {
				executeConsumer.handler(sink::next);
				executeConsumer.completionHandler(MonoUtils.toHandler(registrationSink));
			})
					.flatMap(msg -> td
							.execute(msg.body().getRequest(), msg.body().isExecuteDirectly())
							.map(result -> Tuples.of(msg, result)))
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
					.doOnError(ex -> {
						logger.error("Error when processing a request", ex);
					})
					.subscribe();
		});
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
						.doOnError(ex -> logger.error("Undeploy of bot \"" + botAlias + "\": stop failed", ex))
						.doOnTerminate(() -> logger.info("Undeploy of bot \"" + botAlias + "\": stopped"))));
	}

	private Mono<Void> pipe(AsyncTdDirectImpl td, String botAddress, String botAlias, boolean local) {
		Flux<TdResultList> updatesFlux = td
				.receive(tdOptions)
				.flatMap(item -> Mono.defer(() -> {
					if (item.succeeded()) {
						var tdObject = item.result();
						if (tdObject instanceof Update) {
							var tdUpdate = (Update) tdObject;
							if (tdUpdate.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
								var tdUpdateAuthorizationState = (UpdateAuthorizationState) tdUpdate;
								if (tdUpdateAuthorizationState.authorizationState.getConstructor()
										== AuthorizationStateClosed.CONSTRUCTOR) {
									logger.debug("Undeploying after receiving AuthorizationStateClosed");
									return rxStop().as(MonoUtils::toMono).thenReturn(item);
								}
							}
						} else if (tdObject instanceof Error) {
							// An error in updates means that a fatal error occurred
							logger.debug("Undeploying after receiving a fatal error");
							return rxStop().as(MonoUtils::toMono).thenReturn(item);
						}
						return Mono.just(item);
					} else {
						return Mono.just(item);
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

		//noinspection CallingSubscribeInNonBlockingScope
		updatesFlux
				.flatMap(update -> updatesSender.rxWrite(update).as(MonoUtils::toMono))
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
				})
				.subscribeOn(Schedulers.single())
				.subscribe();
		return Mono.empty();
	}
}
