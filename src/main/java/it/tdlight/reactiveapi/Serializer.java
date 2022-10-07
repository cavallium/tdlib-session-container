package it.tdlight.reactiveapi;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import java.io.Closeable;
import java.util.Map;

public interface Serializer<T> {

	default byte[] serialize(T data) throws IOException {
		try (var baos = new FastByteArrayOutputStream()) {
			try (var daos = new DataOutputStream(baos)) {
				serialize(data, daos);
				baos.trim();
				return baos.array;
			}
		}
	}

	default void serialize(T data, DataOutput output) throws IOException {
		output.write(serialize(data));
	}
}
