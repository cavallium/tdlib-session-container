package it.tdlight.tdlibsession.td.middle;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
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
		try (var os = new ByteBufOutputStream(buffer.getByteBuf())) {
			t.serialize(os);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public T decodeFromWire(int pos, Buffer buffer) {
		try (var is = new ByteBufInputStream(buffer.getByteBuf(), pos)) {
			//noinspection unchecked
			return (T) TdApi.Deserializer.deserialize(is);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
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
