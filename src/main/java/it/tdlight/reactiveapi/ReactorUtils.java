package it.tdlight.reactiveapi;

import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorUtils {

	public static <V> Flux<V> subscribeOnce(Flux<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		});
	}

	public static <V> Mono<V> subscribeOnce(Mono<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		});
	}
}
