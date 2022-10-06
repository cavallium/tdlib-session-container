package it.tdlight.reactiveapi;

import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FutureEventConsumer<K> implements EventConsumer<K> {

	private final Mono<EventConsumer<K>> future;

	public FutureEventConsumer(Mono<EventConsumer<K>> future) {
		this.future = future.cache();
	}

	@Override
	public Flux<Timestamped<K>> consumeMessages() {
		return future.flatMapMany(EventConsumer::consumeMessages);
	}
}
