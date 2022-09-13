package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Instant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveApiMultiClient {

	Flux<ClientBoundEvent> clientBoundEvents();

	<T extends TdApi.Object> Mono<T> request(long userId, TdApi.Function<T> request, Instant timeout);

	Mono<Void> close();

	default ReactiveApiThinClient view(long userId) {
		return new ReactiveApiThinClient() {
			@Override
			public <T extends Object> Mono<T> request(Function<T> request, Instant timeout) {
				return ReactiveApiMultiClient.this.request(userId, request, timeout);
			}

			@Override
			public long getUserId() {
				return userId;
			}
		};
	}
}
