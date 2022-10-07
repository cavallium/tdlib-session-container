package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class TdlibDeserializer implements Deserializer<Object> {

	@Override
	public Object deserialize(byte[] data) {
		if (data.length == 0) {
			return null;
		}
		var bais = new ByteArrayInputStream(data);
		var dais = new DataInputStream(bais);
		return deserialize(-1, dais);
	}

	@Override
	public Object deserialize(int length, DataInput dataInput) {
		try {
			if (dataInput.readInt() != SERIAL_VERSION) {
				return new TdApi.Error(400, "Conflicting protocol version");
			}
			return TdApi.Deserializer.deserialize(dataInput);
		} catch (IOException e) {
			throw new SerializationException("Failed to deserialize TDLib object", e);
		}
	}
}
