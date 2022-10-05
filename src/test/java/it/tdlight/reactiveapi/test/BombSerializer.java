package it.tdlight.reactiveapi.test;

import org.apache.kafka.common.serialization.Serializer;

public final class BombSerializer implements Serializer<Object> {

	@Override
	public byte[] serialize(String topic, Object data) {
		throw new FakeException();
	}
}
