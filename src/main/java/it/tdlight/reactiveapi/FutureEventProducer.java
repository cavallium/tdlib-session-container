package it.tdlight.reactiveapi;

import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class FutureEventProducer<K> implements EventProducer<K> {

	private final Mono<EventProducer<K>> future;

	public FutureEventProducer(Mono<EventProducer<K>> future) {
		this.future = future.cache();
	}

	@Override
	public Mono<Void> sendMessages(Flux<K> eventsFlux) {
		return future.flatMap(ep -> ep.sendMessages(eventsFlux));
	}

	@Override
	public void close() {
		future.doOnNext(EventProducer::close).subscribeOn(Schedulers.parallel()).subscribe();
	}
}
