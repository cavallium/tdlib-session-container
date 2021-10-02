package it.tdlight.tdlibsession.td;

import it.tdlight.common.ReactiveItem;
import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.jni.TdApi;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;

public class WrappedReactorTelegramClient implements ReactorTelegramClient {

	private final ReactiveTelegramClient reactiveTelegramClient;

	public WrappedReactorTelegramClient(ReactiveTelegramClient reactiveTelegramClient) {
		this.reactiveTelegramClient = reactiveTelegramClient;
	}

	@SuppressWarnings("Convert2MethodRef")
	public Mono<Void> initialize() {
		return MonoUtils
				.fromBlockingEmpty(() -> reactiveTelegramClient.createAndRegisterClient());
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return Flux
				.<ReactiveItem>create(sink -> reactiveTelegramClient.subscribe(new CoreSubscriber<>() {
					@Override
					public void onSubscribe(@NotNull Subscription s) {
						sink.onCancel(s::cancel);
						sink.onRequest(s::request);
					}

					@Override
					public void onNext(ReactiveItem reactiveItem) {
						sink.next(reactiveItem);
					}

					@Override
					public void onError(Throwable t) {
						sink.error(t);
					}

					@Override
					public void onComplete() {
						sink.complete();
					}
				}), OverflowStrategy.BUFFER)
				.handle((item, sink) -> {
					if (item.isUpdate()) {
						sink.next(item.getUpdate());
					} else if (item.isHandleException()) {
						sink.error(item.getHandleException());
					} else if (item.isUpdateException()) {
						sink.error(item.getUpdateException());
					} else {
						sink.error(new IllegalStateException("This shouldn't happen. Received unknown ReactiveItem type"));
					}
				});
	}

	/**
	 * Sends a request to the TDLib.
	 *
	 * @param query   Object representing a query to the TDLib.
	 * @param timeout Response timeout.
	 * @return a publisher that will emit exactly one item, or an error
	 * @throws NullPointerException if query is null.
	 */
	@Override
	public Mono<TdApi.Object> send(TdApi.Function query, Duration timeout) {
		return Mono.from(reactiveTelegramClient.send(query, timeout)).single();
	}

	/**
	 * Synchronously executes a TDLib request. Only a few marked accordingly requests can be executed synchronously.
	 *
	 * @param query Object representing a query to the TDLib.
	 * @return request result or {@link TdApi.Error}.
	 * @throws NullPointerException if query is null.
	 */
	@Override
	public TdApi.Object execute(TdApi.Function query) {
		return reactiveTelegramClient.execute(query);
	}
}
