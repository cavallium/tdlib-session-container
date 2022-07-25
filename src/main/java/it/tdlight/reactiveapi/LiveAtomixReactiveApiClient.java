package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final Flux<ClientBoundEvent> clientBoundEvents;

	LiveAtomixReactiveApiClient(KafkaTdlibClient kafkaTdlibClient, long userId, String subGroupId) {
		super(kafkaTdlibClient, userId);
		this.clientBoundEvents = kafkaTdlibClient.events()
				.consumeMessages(subGroupId, userId)
				.map(Timestamped::data);
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

}
