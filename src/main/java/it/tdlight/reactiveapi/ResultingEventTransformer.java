package it.tdlight.reactiveapi;

import reactor.core.publisher.Flux;


public interface ResultingEventTransformer {

	Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events);
}
