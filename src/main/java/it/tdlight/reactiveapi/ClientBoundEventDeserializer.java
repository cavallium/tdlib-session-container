package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import org.apache.kafka.common.serialization.Deserializer;

public class ClientBoundEventDeserializer implements Deserializer<ClientBoundEvent> {

	@Override
	public ClientBoundEvent deserialize(String topic, byte[] data) {
		return LiveAtomixReactiveApiClient.deserializeEvent(data);
	}
}
