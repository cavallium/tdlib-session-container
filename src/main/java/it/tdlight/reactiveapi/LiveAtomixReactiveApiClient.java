package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;

public class LiveAtomixReactiveApiClient extends BaseAtomixReactiveApiClient {

	private final ClientsSharedTdlib kafkaSharedTdlibClients;

	LiveAtomixReactiveApiClient(ClientsSharedTdlib kafkaSharedTdlibClients) {
		super(kafkaSharedTdlibClients);
		this.kafkaSharedTdlibClients = kafkaSharedTdlibClients;
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents(String lane) {
		return kafkaSharedTdlibClients.events(lane).map(Timestamped::data);
	}

	@Override
	public Map<String, Flux<ClientBoundEvent>> clientBoundEvents() {
		return kafkaSharedTdlibClients.events().entrySet().stream()
				.collect(Collectors.toUnmodifiableMap(Entry::getKey, e -> e.getValue().map(Timestamped::data)));
	}
}
