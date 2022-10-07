package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TdlibResponseSerializer<T extends TdApi.Object> implements Serializer<OnResponse<T>> {

	@Override
	public byte[] serialize(OnResponse<T> data) {
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
	public void serialize(OnResponse<T> data, DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(data.clientId());
		dataOutput.writeLong(data.requestId());
		dataOutput.writeLong(data.userId());
		dataOutput.writeInt(SERIAL_VERSION);
		if (data instanceof Response<?> response) {
			response.response().serialize(dataOutput);
		} else if (data instanceof OnResponse.InvalidResponse<T>) {
			dataOutput.writeLong(-2);
		} else {
			throw new SerializationException("Unknown response type: " + dataOutput.getClass());
		}
	}
}
