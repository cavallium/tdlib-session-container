package it.tdlight.reactiveapi.kafka;

import it.tdlight.reactiveapi.Deserializer;
import it.tdlight.reactiveapi.SerializationException;
import it.tdlight.reactiveapi.Serializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class KafkaDeserializer<T> implements Deserializer<T>, org.apache.kafka.common.serialization.Deserializer<T> {

	private Deserializer<T> deserializer;

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		var clazz = (Class<?>) configs.get("custom.deserializer.class");
		try {
			this.deserializer = (Deserializer<T>) clazz.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		try {
			return deserializer.deserialize(data);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(byte[] data) throws IOException {
		return deserializer.deserialize(data);
	}
}
