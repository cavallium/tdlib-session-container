package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Instant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveApiMultiClient {

	Flux<ClientBoundEvent> clientBoundEvents();

	<T extends TdApi.Object> Mono<T> request(long userId, long liveId, TdApi.Function<T> request, Instant timeout);

	void close();
}
