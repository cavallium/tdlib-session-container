package it.tdlight.reactiveapi;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

public interface EventConsumer<K> {

	ChannelCodec getChannelCodec();

	String getChannelName();

	Flux<Timestamped<K>> consumeMessages();
}
