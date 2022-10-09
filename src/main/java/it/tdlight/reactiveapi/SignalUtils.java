package it.tdlight.reactiveapi;

import it.tdlight.common.Signal;
import it.tdlight.jni.TdApi;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class SignalUtils {

	private static final Serializer<Signal> SERIALIZER = new Serializer<>() {
		@Override
		public byte[] serialize(Signal data) {
			return SignalUtils.serialize(data);
		}
	};

	private static final Deserializer<Signal> DESERIALIZER = new Deserializer<>() {
		@Override
		public Signal deserialize(byte[] data) {
			return SignalUtils.deserialize(data);
		}
	};

	public static Serializer<Signal> serializer() {
		return SERIALIZER;
	}

	public static Deserializer<Signal> deserializer() {
		return DESERIALIZER;
	}

	public static Signal deserialize(byte[] bytes) {
		var dis = new DataInputStream(new FastByteArrayInputStream(bytes));
		try {
			byte type = dis.readByte();
			return switch (type) {
				case 0 -> Signal.ofUpdate(TdApi.Deserializer.deserialize(dis));
				case 1 -> Signal.ofUpdateException(new Exception(dis.readUTF()));
				case 2 -> Signal.ofClosed();
				default -> throw new IllegalStateException("Unexpected value: " + type);
			};
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static byte[] serialize(Signal signal) {
		var baos = new FastByteArrayOutputStream();
		try (var daos = new DataOutputStream(baos)) {
			if (signal.isUpdate()) {
				daos.writeByte(0);
				var up = signal.getUpdate();
				up.serialize(daos);
			} else if (signal.isException()) {
				daos.writeByte(1);
				var ex = signal.getException();
				var exMsg = ex.getMessage();
				daos.writeUTF(exMsg);
			} else if (signal.isClosed()) {
				daos.writeByte(2);
			} else {
				throw new IllegalStateException();
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		baos.trim();
		return baos.array;
	}
}
