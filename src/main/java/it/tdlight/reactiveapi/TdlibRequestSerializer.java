package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class TdlibRequestSerializer<T extends TdApi.Object> implements Serializer<OnRequest<T>> {

	private static final Instant INFINITE_TIMEOUT = Instant.now().plus(100_000, ChronoUnit.DAYS);

	@Override
	public byte[] serialize(String topic, OnRequest<T> data) {
		try {
			if (data == null) {
				return new byte[0];
			} else {
				try(var baos = new ByteArrayOutputStream()) {
					try (var daos = new DataOutputStream(baos)) {
						daos.writeLong(data.clientId());
						daos.writeLong(data.requestId());
						daos.writeInt(SERIAL_VERSION);
						if (data instanceof OnRequest.Request<?> request) {
							if (request.timeout() == Instant.MAX || request.timeout().compareTo(INFINITE_TIMEOUT) >= 0) {
								daos.writeLong(-1);
							} else {
								daos.writeLong(request.timeout().toEpochMilli());
							}
							request.request().serialize(daos);
						} else if (data instanceof OnRequest.InvalidRequest<?>) {
							daos.writeLong(-2);
						} else {
							throw new SerializationException("Unknown request type: " + daos.getClass());
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
