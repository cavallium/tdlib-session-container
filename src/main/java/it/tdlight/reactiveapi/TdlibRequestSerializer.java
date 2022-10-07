package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TdlibRequestSerializer<T extends TdApi.Object> implements Serializer<OnRequest<T>> {

	private static final Instant INFINITE_TIMEOUT = Instant.now().plus(100_000, ChronoUnit.DAYS);

	@Override
	public byte[] serialize(OnRequest<T> data) {
		try {
			if (data == null) {
				return new byte[0];
			} else {
				try(var baos = new ByteArrayOutputStream()) {
					try (var daos = new DataOutputStream(baos)) {
						serialize(data, daos);
						daos.flush();
						return baos.toByteArray();
					}
				}
			}
		} catch (IOException e) {
			throw new SerializationException("Failed to serialize TDLib object", e);
		}
	}

	@Override
	public void serialize(OnRequest<T> data, DataOutput dataOutput) throws IOException {
		if (data == null) {
			return;
		}
		dataOutput.writeLong(data.userId());
		dataOutput.writeLong(data.clientId());
		dataOutput.writeLong(data.requestId());
		dataOutput.writeInt(SERIAL_VERSION);
		if (data instanceof OnRequest.Request<?> request) {
			if (request.timeout() == Instant.MAX || request.timeout().compareTo(INFINITE_TIMEOUT) >= 0) {
				dataOutput.writeLong(-1);
			} else {
				dataOutput.writeLong(request.timeout().toEpochMilli());
			}
			request.request().serialize(dataOutput);
		} else if (data instanceof OnRequest.InvalidRequest<?>) {
			dataOutput.writeLong(-2);
		} else {
			throw new SerializationException("Unknown request type: " + dataOutput.getClass());
		}
	}
}
