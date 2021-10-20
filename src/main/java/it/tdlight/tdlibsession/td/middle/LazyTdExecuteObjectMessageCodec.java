package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;

public class LazyTdExecuteObjectMessageCodec<T extends TdApi.Object>
		implements MessageCodec<ExecuteObject<T>, ExecuteObject<T>> {

	private static final TdExecuteObjectMessageCodec<?> realCodec = new TdExecuteObjectMessageCodec<>();

	public LazyTdExecuteObjectMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, ExecuteObject t) {
		realCodec.encodeToWire(buffer, t);
	}

	@Override
	public ExecuteObject<T> decodeFromWire(int pos, Buffer buffer) {
		return new ExecuteObject<>(pos, buffer);
	}

	@Override
	public ExecuteObject<T> transform(ExecuteObject t) {
		// If a message is sent *locally* across the event bus.
		// This sends message just as is
		return t;
	}

	@Override
	public String name() {
		return "ExecuteObjectCodec";
	}

	@Override
	public byte systemCodecID() {
		// Always -1
		return -1;
	}
}
