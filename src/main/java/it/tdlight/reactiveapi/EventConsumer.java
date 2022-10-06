package it.tdlight.reactiveapi;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

public interface EventConsumer<K> {

	Flux<Timestamped<K>> consumeMessages();
}
