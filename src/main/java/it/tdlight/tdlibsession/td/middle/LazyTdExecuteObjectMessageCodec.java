package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class LazyTdExecuteObjectMessageCodec implements MessageCodec<ExecuteObject, ExecuteObject> {

	private static final TdExecuteObjectMessageCodec realCodec = new TdExecuteObjectMessageCodec();

	public LazyTdExecuteObjectMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, ExecuteObject t) {
		realCodec.encodeToWire(buffer, t);
	}

	@Override
	public ExecuteObject decodeFromWire(int pos, Buffer buffer) {
		return new ExecuteObject(pos, buffer);
	}

	@Override
	public ExecuteObject transform(ExecuteObject t) {
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
