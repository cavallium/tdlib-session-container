package it.tdlight.tdlibsession.td.direct;

import it.tdlight.common.TelegramClient;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Close;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.Ok;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlight.ClientManager;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AsyncTdDirectImpl implements AsyncTdDirect {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdDirect.class);

	private final One<TelegramClient> td = Sinks.one();
	private final Scheduler tdScheduler = Schedulers.newSingle("TdMain");

	private final String botAlias;

	public AsyncTdDirectImpl(String botAlias) {
		this.botAlias = botAlias;
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean synchronous) {
		if (synchronous) {
			return td.asMono().flatMap(td -> Mono.fromCallable(() -> {
				if (td != null) {
					return TdResult.<T>of(td.execute(request));
				} else {
					if (request.getConstructor() == Close.CONSTRUCTOR) {
						return TdResult.<T>of(new Ok());
					}
					throw new IllegalStateException("TDLib client is destroyed");
				}
			}).publishOn(Schedulers.boundedElastic()).single());
		} else {
			return td.asMono().flatMap(td -> Mono.<TdResult<T>>create(sink -> {
				if (td != null) {
					try {
						td.send(request, v -> sink.success(TdResult.of(v)), sink::error);
					} catch (Throwable t) {
						sink.error(t);
					}
				} else {
					if (request.getConstructor() == Close.CONSTRUCTOR) {
						sink.success(TdResult.<T>of(new Ok()));
					}
					sink.error(new IllegalStateException("TDLib client is destroyed"));
				}
			})).single();
		}
	}

	@Override
	public Flux<TdResult<TdApi.Object>> receive(AsyncTdDirectOptions options) {
		return Flux.<TdResult<TdApi.Object>>create(emitter -> {
			One<java.lang.Object> closedFromTd = Sinks.one();
			var client = ClientManager.create((Object object) -> {
				emitter.next(TdResult.of(object));
				// Close the emitter if receive closed state
				if (object.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR
						&& ((UpdateAuthorizationState) object).authorizationState.getConstructor()
						== AuthorizationStateClosed.CONSTRUCTOR) {
					closedFromTd.tryEmitValue(new java.lang.Object());
					emitter.complete();
				}
			}, emitter::error, emitter::error);
			try {
				this.td.tryEmitValue(client).orThrow();
			} catch (Exception ex) {
				emitter.error(ex);
			}

			// Send close if the stream is disposed before tdlib is closed
			emitter.onDispose(() -> {
				closedFromTd.asMono().take(Duration.ofMillis(10)).switchIfEmpty(Mono.fromRunnable(() -> client.send(new Close(),
						result -> logger.trace("Close result: {}", result),
						ex -> logger.trace("Error when disposing td client", ex)
				))).subscribe();
			});
		});
	}
}
