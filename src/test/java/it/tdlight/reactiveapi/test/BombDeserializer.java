package it.tdlight.reactiveapi.test;

import org.apache.kafka.common.serialization.Deserializer;

public final class BombDeserializer implements Deserializer<Object> {

	@Override
	public Object deserialize(String topic, byte[] data) {
		throw new FakeException();
	}
}
