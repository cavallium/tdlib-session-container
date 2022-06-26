package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class TdlibSerializer implements Serializer<TdApi.Object> {

	@Override
	public byte[] serialize(String topic, TdApi.Object data) {
		try {
			if (data == null) {
				return new byte[0];
			} else {
				try(var baos = new ByteArrayOutputStream()) {
					try (var daos = new DataOutputStream(baos)) {
						daos.writeInt(SERIAL_VERSION);
						data.serialize(daos);
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
