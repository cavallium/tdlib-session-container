package it.tdlight.reactiveapi;

import io.atomix.core.Atomix;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final Flux<ClientBoundEvent> clientBoundEvents;
	private final Mono<Long> liveId;

	LiveAtomixReactiveApiClient(Atomix atomix,
			KafkaConsumer kafkaConsumer,
			long liveId,
			long userId,
			String subGroupId) {
		super(atomix, userId);
		this.clientBoundEvents = kafkaConsumer.consumeMessages(subGroupId, userId, liveId).share();
		this.liveId = Mono.just(liveId);
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

	@Override
	public Mono<Long> resolveLiveId() {
		return liveId;
	}

}
