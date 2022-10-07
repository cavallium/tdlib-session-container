package it.tdlight.reactiveapi;

import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class UtfCodec implements Serializer<String>, Deserializer<String> {

	@Override
	public String deserialize(byte[] data) {
		return new String(data, StandardCharsets.UTF_8);
	}

	@Override
	public byte[] serialize(String data) {
		return data.getBytes(StandardCharsets.UTF_8);
	}

}
