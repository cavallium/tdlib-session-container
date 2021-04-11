package it.tdlight.tdlibsession.td;

import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
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
	 * @param query            Object representing a query to the TDLib.
	 * @throws NullPointerException if query is null.
	 * @return a publisher that will emit exactly one item, or an error
	 */
	@Override
	public <T extends TdApi.Object> Mono<T> send(TdApi.Function query) {
		return Flux.from(reactiveTelegramClient.send(query)).single().handle((item, sink) -> {
			if (item.getConstructor() == Error.CONSTRUCTOR) {
				var error = ((TdApi.Error) item);
				sink.error(new TdError(error.code, error.message));
			} else {
				//noinspection unchecked
				sink.next((T) item);
			}
		});
	}

	/**
	 * Synchronously executes a TDLib request. Only a few marked accordingly requests can be executed synchronously.
	 *
	 * @param query Object representing a query to the TDLib.
	 * @return request result or {@link TdApi.Error}.
	 * @throws NullPointerException if query is null.
	 */
	@Override
	public <T extends TdApi.Object> T execute(TdApi.Function query) {
		//noinspection unchecked
		return (T) reactiveTelegramClient.execute(query);
	}
}
