package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class TdlibResponseSerializer<T extends TdApi.Object> implements Serializer<OnResponse<T>> {

	@Override
	public byte[] serialize(String topic, OnResponse<T> data) {
		try {
			if (data == null) {
				return new byte[0];
			} else {
				try(var baos = new ByteArrayOutputStream()) {
					try (var daos = new DataOutputStream(baos)) {
						daos.writeLong(data.clientId());
						daos.writeLong(data.requestId());
						daos.writeLong(data.userId());
						daos.writeInt(SERIAL_VERSION);
						if (data instanceof Response<?> response) {
							response.response().serialize(daos);
						} else if (data instanceof OnResponse.InvalidResponse<T>) {
							daos.writeLong(-2);
						} else {
							throw new SerializationException("Unknown response type: " + daos.getClass());
						}
						daos.flush();
						return baos.toByteArray();
					}
				}
			}
		} catch (IOException e) {
			throw new SerializationException("Failed to serialize TDLib object", e);
		}
	}
}
