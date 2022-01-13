package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import org.apache.kafka.common.serialization.Serializer;

public class ClientBoundEventSerializer implements Serializer<ClientBoundEvent> {

	@Override
	public byte[] serialize(String topic, ClientBoundEvent data) {
		return ReactiveApiPublisher.serializeEvent(data);
	}
}
