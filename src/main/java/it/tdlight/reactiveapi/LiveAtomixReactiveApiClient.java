package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import reactor.core.publisher.Flux;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final Flux<ClientBoundEvent> clientBoundEvents;

	LiveAtomixReactiveApiClient(KafkaSharedTdlibClients kafkaSharedTdlibClients) {
		super(kafkaSharedTdlibClients);
		this.clientBoundEvents = kafkaSharedTdlibClients.events().map(Timestamped::data);
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

}
