package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.InvalidResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;

public class TdlibResponseDeserializer<T extends TdApi.Object> implements Deserializer<OnResponse<T>> {

	@Override
	public OnResponse<T> deserialize(byte[] data) {
		if (data.length == 0) {
			return null;
		}
		try {
			var bais = new ByteArrayInputStream(data);
			var dais = new DataInputStream(bais);
			return deserialize(-1, dais);
		} catch (UnsupportedOperationException | IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public OnResponse<T> deserialize(int length, DataInput dataInput) throws IOException {
		var clientId = dataInput.readLong();
		var requestId = dataInput.readLong();
		var userId = dataInput.readLong();
		if (dataInput.readInt() != SERIAL_VERSION) {
			// Deprecated response
			return new InvalidResponse<>(clientId, requestId, userId);
		} else {
			@SuppressWarnings("unchecked")
			T response = (T) TdApi.Deserializer.deserialize(dataInput);
			return new Response<>(clientId, requestId, userId, response);
		}
	}
}
