package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TdlibDeserializer implements Deserializer<Object> {

	@Override
	public Object deserialize(String topic, byte[] data) {
		if (data.length == 0) {
			return null;
		}
		var bais = new ByteArrayInputStream(data);
		var dais = new DataInputStream(bais);
		try {
			if (dais.readInt() != SERIAL_VERSION) {
				return new TdApi.Error(400, "Conflicting protocol version");
			}
			return TdApi.Deserializer.deserialize(dais);
		} catch (IOException e) {
			throw new SerializationException("Failed to deserialize TDLib object", e);
		}
	}
}
