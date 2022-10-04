package it.tdlight.reactiveapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventProducer<K> {

	ChannelCodec getChannelCodec();

	String getChannelName();

	Mono<Void> sendMessages(Flux<K> eventsFlux);

	void close();
}
