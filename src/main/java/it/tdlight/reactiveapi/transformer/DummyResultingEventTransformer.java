package it.tdlight.reactiveapi.transformer;

import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import reactor.core.publisher.Flux;

public class DummyResultingEventTransformer implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events;
	}
}
