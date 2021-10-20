package it.tdlight.tdlibsession.td;

import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.common.Signal;
import it.tdlight.common.SignalListener;
import it.tdlight.common.UpdatesHandler;
import it.tdlight.jni.TdApi;
import it.tdlight.utils.MonoUtils;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class WrappedReactorTelegramClient implements ReactorTelegramClient {

	private final ReactiveTelegramClient reactiveTelegramClient;
	private final AtomicReference<Flux<Signal>> multicastSignals = new AtomicReference<>(null);

	public WrappedReactorTelegramClient(ReactiveTelegramClient reactiveTelegramClient) {
		this.reactiveTelegramClient = reactiveTelegramClient;
	}

	public Mono<Void> initialize() {
		return MonoUtils
				.fromBlockingEmpty(() -> {
					reactiveTelegramClient.createAndRegisterClient();
					Flux<Signal> signalsFlux = Flux
							.<Signal>create(sink -> {
								reactiveTelegramClient.setListener(sink::next);
								sink.onCancel(reactiveTelegramClient::cancel);
								sink.onDispose(reactiveTelegramClient::dispose);
							}, OverflowStrategy.BUFFER)
							.subscribeOn(Schedulers.boundedElastic())
							.takeWhile(Signal::isNotClosed);
					Flux<Signal> refCountedSharedSignalsFlux = signalsFlux.publish().refCount();
					multicastSignals.set(refCountedSharedSignalsFlux);
				});
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return Flux
				.defer(() -> {
					Flux<Signal> flux = multicastSignals.get();
					if (flux == null) {
						return Flux.error(new IllegalStateException("TDLib session not started"));
					} else {
						return flux;
					}
				})
				.handle((item, sink) -> {
					if (item.isUpdate()) {
						sink.next(item.getUpdate());
					} else if (item.isException()) {
						sink.error(item.getException());
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
	public <T extends TdApi.Object> Mono<TdApi.Object> send(TdApi.Function<T> query, Duration timeout) {
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
	public <T extends TdApi.Object> TdApi.Object execute(TdApi.Function<T> query) {
		return reactiveTelegramClient.execute(query);
	}
}
