package it.tdlight.reactiveapi;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

public interface EventConsumer<K> {

	boolean isQuickResponse();

	ChannelCodec getChannelCodec();

	String getChannelName();

	Flux<Timestamped<K>> consumeMessages(@NotNull String subGroupId);
}
