package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.json.JsonObject;
import it.tdlight.common.TelegramClient;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Close;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Ok;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.utils.MonoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;

public class AsyncTdDirectImpl implements AsyncTdDirect {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdDirect.class);

	private final TelegramClientFactory telegramClientFactory;
	private final JsonObject implementationDetails;
	private final String botAlias;

	private final One<TelegramClient> td = Sinks.one();

	public AsyncTdDirectImpl(TelegramClientFactory telegramClientFactory,
			JsonObject implementationDetails,
			String botAlias) {
		this.telegramClientFactory = telegramClientFactory;
		this.implementationDetails = implementationDetails;
		this.botAlias = botAlias;
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean synchronous) {
		if (synchronous) {
			return td.asMono().single().flatMap(td -> MonoUtils.fromBlockingSingle(() -> {
				if (td != null) {
					return TdResult.<T>of(td.execute(request));
				} else {
					if (request.getConstructor() == Close.CONSTRUCTOR) {
						return TdResult.<T>of(new Ok());
					}
					throw new IllegalStateException("TDLib client is destroyed");
				}
			}));
		} else {
			return td.asMono().single().flatMap(td -> Mono.<TdResult<T>>create(sink -> {
				if (td != null) {
					td.send(request, v -> sink.success(TdResult.of(v)), sink::error);
				} else {
					if (request.getConstructor() == Close.CONSTRUCTOR) {
						logger.trace("Sending close success to sink " + sink.toString());
						sink.success(TdResult.<T>of(new Ok()));
					} else {
						logger.trace("Sending close error to sink " + sink.toString());
						sink.error(new IllegalStateException("TDLib client is destroyed"));
					}
				}
			})).single();
		}
	}

	@Override
	public Flux<TdApi.Object> receive(AsyncTdDirectOptions options) {
		// If closed it will be either true or false
		final One<Boolean> closedFromTd = Sinks.one();
		return telegramClientFactory.create(implementationDetails)
				.flatMapMany(client -> Flux
						.<TdApi.Object>create(updatesSink -> {
							client.initialize((TdApi.Object object) -> {
								updatesSink.next(object);
								// Close the emitter if receive closed state
								if (object.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR
										&& ((UpdateAuthorizationState) object).authorizationState.getConstructor()
										== AuthorizationStateClosed.CONSTRUCTOR) {
									logger.debug("Received closed status from tdlib");
									closedFromTd.tryEmitValue(true);
									updatesSink.complete();
								}
							}, updatesSink::error, updatesSink::error);

							if (td.tryEmitValue(client).isFailure()) {
								updatesSink.error(new TdError(500, "Failed to emit td client"));
							}

							// Send close if the stream is disposed before tdlib is closed
							updatesSink.onDispose(() -> {
								// Try to emit false, so that if it has not been closed from tdlib, now it is explicitly false.
								closedFromTd.tryEmitValue(false);

								closedFromTd.asMono().filter(isClosedFromTd -> !isClosedFromTd).doOnNext(x -> {
									logger.warn("The stream has been disposed without closing tdlib. Sending TdApi.Close()...");
									client.send(new Close(),
											result -> logger.warn("Close result: {}", result),
											ex -> logger.error("Error when disposing td client", ex)
									);
								}).subscribeOn(Schedulers.single()).subscribe();
							});
						})
						.subscribeOn(Schedulers.boundedElastic())
				);
	}
}
