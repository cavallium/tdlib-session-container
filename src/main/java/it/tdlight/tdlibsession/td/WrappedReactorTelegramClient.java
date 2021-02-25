package it.tdlight.tdlibsession.td;

import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.jni.TdApi;
import it.tdlight.utils.MonoUtils;
import reactor.core.publisher.Flux;
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
				.from(reactiveTelegramClient)
				.concatMap(item -> {
					if (item.isUpdate()) {
						return Mono.just(item.getUpdate());
					} else if (item.isHandleException()) {
						return Mono.error(item.getHandleException());
					} else if (item.isUpdateException()) {
						return Mono.error(item.getUpdateException());
					} else {
						return Mono.error(new IllegalStateException("This shouldn't happen. Received unknown ReactiveItem type"));
					}
				});
	}

	/**
	 * Sends a request to the TDLib.
	 *
	 * @param query            Object representing a query to the TDLib.
	 * @throws NullPointerException if query is null.
	 * @return a publisher that will emit exactly one item, or an error
	 */
	@Override
	public Mono<TdApi.Object> send(TdApi.Function query) {
		return Mono.from(reactiveTelegramClient.send(query));
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
