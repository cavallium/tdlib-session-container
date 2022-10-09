package it.tdlight.reactiveapi.rsocket;

import it.cavallium.filequeue.Deserializer;
import it.cavallium.filequeue.Serializer;
import it.tdlight.reactiveapi.ClientBoundEventDeserializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileQueueUtils {

	public static <T> Serializer<T> convert(it.tdlight.reactiveapi.Serializer<T> serializer) {
		return new Serializer<T>() {
			@Override
			public byte[] serialize(T data) throws IOException {
				return serializer.serialize(data);
			}

			@Override
			public void serialize(T data, DataOutput output) throws IOException {
				serializer.serialize(data, output);
			}
		};
	}

	public static <T> Deserializer<T> convert(it.tdlight.reactiveapi.Deserializer<T> deserializer) {
		return new Deserializer<T>() {
			@Override
			public T deserialize(byte[] data) throws IOException {
				return deserializer.deserialize(data);
			}

			@Override
			public T deserialize(int length, DataInput dataInput) throws IOException {
				return deserializer.deserialize(length, dataInput);
			}
		};
	}
}
