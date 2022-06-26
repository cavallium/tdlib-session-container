package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.InvalidRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class TdlibRequestDeserializer<T extends TdApi.Object> implements Deserializer<OnRequest<T>> {

	@Override
	public OnRequest<T> deserialize(String topic, byte[] data) {
		if (data.length == 0) {
			return null;
		}
		try {
			var bais = new ByteArrayInputStream(data);
			var dais = new DataInputStream(bais);
			var clientId = dais.readLong();
			var requestId = dais.readLong();
			if (dais.readInt() != SERIAL_VERSION) {
				// Deprecated request
				return new InvalidRequest<>(clientId, requestId);
			} else {
				long millis = dais.readLong();
				Instant timeout;
				if (millis == -1) {
					timeout = Instant.ofEpochMilli(Long.MAX_VALUE);
				} else {
					timeout = Instant.ofEpochMilli(millis);
				}
				@SuppressWarnings("unchecked")
				TdApi.Function<T> request = (TdApi.Function<T>) TdApi.Deserializer.deserialize(dais);
				return new Request<>(clientId, requestId, request, timeout);
			}
		} catch (UnsupportedOperationException | IOException e) {
			throw new SerializationException(e);
		}
	}
}
