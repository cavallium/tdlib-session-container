package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class TdlibSerializer implements Serializer<TdApi.Object> {

	@Override
	public byte[] serialize(TdApi.Object data) {
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
	public void serialize(Object data, DataOutput output) throws IOException {
		if (data == null) {
			return;
		}
		output.writeInt(SERIAL_VERSION);
		data.serialize(output);
	}
}
