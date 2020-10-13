package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TdMessageCodec<T extends TdApi.Object> implements MessageCodec<T, T> {

	private final Class<T> clazz;
	private final String codecName;

	public TdMessageCodec(Class<T> clazz) {
		super();
		this.clazz = clazz;
		this.codecName = clazz.getSimpleName() + "TdCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, T t) {
		try (var bos = new FastByteArrayOutputStream()) {
			try (var dos = new DataOutputStream(bos)) {
				t.serialize(dos);
			}
			bos.trim();
			buffer.appendBytes(bos.array);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public T decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new FastByteArrayInputStream(buffer.getBytes(pos, buffer.length()))) {
			try (var dis = new DataInputStream(fis)) {
				//noinspection unchecked
				return (T) TdApi.Deserializer.deserialize(dis);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return null;
	}

	@Override
	public T transform(T t) {
		// If a message is sent *locally* across the event bus.
		// This sends message just as is
		return t;
	}

	@Override
	public String name() {
		return codecName;
	}

	@Override
	public byte systemCodecID() {
		// Always -1
		return -1;
	}
}
