package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.io.DataInput;
import java.io.IOException;

public class ClientBoundEventDeserializer implements Deserializer<ClientBoundEvent> {

	@Override
	public ClientBoundEvent deserialize(byte[] data) {
		if (data == null || data.length == 0) {
			return null;
		}
		return LiveAtomixReactiveApiClient.deserializeEvent(data);
	}

	@Override
	public ClientBoundEvent deserialize(int length, DataInput dataInput) throws IOException {
		if (dataInput == null || length == 0) {
			return null;
		}
		return LiveAtomixReactiveApiClient.deserializeEvent(dataInput);
	}
}
