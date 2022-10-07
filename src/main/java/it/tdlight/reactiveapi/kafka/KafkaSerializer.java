package it.tdlight.reactiveapi.kafka;

import it.tdlight.reactiveapi.Serializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;

public class KafkaSerializer<T> implements Serializer<T>, org.apache.kafka.common.serialization.Serializer<T> {

	private Serializer<T> serializer;

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		var clazz = (Class<?>) configs.get("custom.serializer.class");
		try {
			this.serializer = (Serializer<T>) clazz.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public byte[] serialize(String topic, T data) {
		try {
			return serializer.serialize(data);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte[] serialize(T data) throws IOException {
		return serializer.serialize(data);
	}
}
