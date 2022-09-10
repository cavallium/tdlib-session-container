package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import reactor.core.publisher.Flux;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final Flux<ClientBoundEvent> clientBoundEvents;

	LiveAtomixReactiveApiClient(KafkaSharedTdlibClients kafkaSharedTdlibClients, long userId) {
		super(kafkaSharedTdlibClients, userId);
		this.clientBoundEvents = kafkaSharedTdlibClients.events(userId).map(Timestamped::data);
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

}
