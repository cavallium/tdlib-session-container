package it.tdlight.reactiveapi;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;

public abstract class SimpleEventProducer<K> implements EventProducer<K> {

	private AtomicReference<Subscription> closeRequest = new AtomicReference<>();

	@Override
	public final Mono<Void> sendMessages(Flux<K> eventsFlux) {
		return handleSendMessages(eventsFlux).doOnSubscribe(s -> closeRequest.set(s));
	}

	public abstract Mono<Void> handleSendMessages(Flux<K> eventsFlux);

	@Override
	public final void close() {
		var s = closeRequest.get();
		if (s != null) {
			s.cancel();
		}
	}
}
