package it.tdlight.reactiveapi;

import io.atomix.core.Atomix;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final Flux<ClientBoundEvent> clientBoundEvents;
	private final long liveId;

	LiveAtomixReactiveApiClient(Atomix atomix,
			KafkaConsumer kafkaConsumer,
			long liveId,
			long userId,
			String subGroupId) {
		super(atomix, userId);
		this.clientBoundEvents = kafkaConsumer
				.consumeMessages(subGroupId, userId, liveId)
				.map(TimestampedClientBoundEvent::event);
		this.liveId = liveId;
		super.initialize();
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

	@Override
	protected Flux<Long> liveIdChange() {
		return Flux.just(liveId);
	}
}
