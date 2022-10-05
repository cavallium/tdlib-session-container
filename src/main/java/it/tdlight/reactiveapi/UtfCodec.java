package it.tdlight.reactiveapi;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class UtfCodec implements Serializer<String>, Deserializer<String> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public String deserialize(String topic, byte[] data) {
		return new String(data, StandardCharsets.UTF_8);
	}

	@Override
	public byte[] serialize(String topic, String data) {
		return data.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public void close() {
	}
}
