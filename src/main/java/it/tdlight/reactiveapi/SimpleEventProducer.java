package it.tdlight.reactiveapi;

import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;

public abstract class SimpleEventProducer<K> implements EventProducer<K> {

	private final Empty<Void> closeRequest = Sinks.empty();

	@Override
	public final Mono<Void> sendMessages(Flux<K> eventsFlux) {
		return handleSendMessages(eventsFlux.takeUntilOther(closeRequest.asMono()));
	}

	public abstract Mono<Void> handleSendMessages(Flux<K> eventsFlux);

	@Override
	public final void close() {
		closeRequest.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
	}
}
