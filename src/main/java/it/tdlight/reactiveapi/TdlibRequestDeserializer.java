package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.InvalidRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TdlibRequestDeserializer<T extends TdApi.Object> implements Deserializer<OnRequest<T>> {

	@Override
	public OnRequest<T> deserialize(byte[] data) {
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
	public OnRequest<T> deserialize(int length, DataInput dataInput) throws IOException {
		var userId = dataInput.readLong();
		var clientId = dataInput.readLong();
		var requestId = dataInput.readLong();
		if (dataInput.readInt() != SERIAL_VERSION) {
			// Deprecated request
			return new InvalidRequest<>(userId, clientId, requestId);
		} else {
			long millis = dataInput.readLong();
			Instant timeout;
			if (millis == -1) {
				timeout = Instant.ofEpochMilli(Long.MAX_VALUE);
			} else {
				timeout = Instant.ofEpochMilli(millis);
			}
			@SuppressWarnings("unchecked")
			TdApi.Function<T> request = (TdApi.Function<T>) TdApi.Deserializer.deserialize(dataInput);
			return new Request<>(userId, clientId, requestId, request, timeout);
		}
	}
}
