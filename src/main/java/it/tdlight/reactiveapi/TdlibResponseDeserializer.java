package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.InvalidResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TdlibResponseDeserializer<T extends TdApi.Object> implements Deserializer<OnResponse<T>> {

	@Override
	public OnResponse<T> deserialize(String topic, byte[] data) {
		if (data.length == 0) {
			return null;
		}
		try {
			var bais = new ByteArrayInputStream(data);
			var dais = new DataInputStream(bais);
			var clientId = dais.readLong();
			var requestId = dais.readLong();
			var userId = dais.readLong();
			if (dais.readInt() != SERIAL_VERSION) {
				// Deprecated response
				return new InvalidResponse<>(clientId, requestId, userId);
			} else {
				@SuppressWarnings("unchecked")
				T response = (T) TdApi.Deserializer.deserialize(dais);
				return new Response<>(clientId, requestId, userId, response);
			}
		} catch (UnsupportedOperationException | IOException e) {
			throw new SerializationException(e);
		}
	}
}
