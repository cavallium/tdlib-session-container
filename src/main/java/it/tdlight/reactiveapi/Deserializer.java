package it.tdlight.reactiveapi;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

public interface Deserializer<T> {

	default T deserialize(byte[] data) throws IOException {
		var bais = new ByteArrayInputStream(data);
		return deserialize(data.length, new DataInputStream(bais));
	}

	default T deserialize(int length, DataInput dataInput) throws IOException {
		byte[] data = new byte[length];
		dataInput.readFully(data);
		return deserialize(data);
	}
}
