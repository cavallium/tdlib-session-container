package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.io.DataOutput;
import java.io.IOException;

public class ClientBoundEventSerializer implements Serializer<ClientBoundEvent> {

	@Override
	public byte[] serialize(ClientBoundEvent data) {
		if (data == null) {
			return null;
		}
		return ReactiveApiPublisher.serializeEvent(data);
	}

	@Override
	public void serialize(ClientBoundEvent data, DataOutput output) throws IOException {
		if (data == null) {
			return;
		}
		ReactiveApiPublisher.writeClientBoundEvent(data, output);
	}
}
