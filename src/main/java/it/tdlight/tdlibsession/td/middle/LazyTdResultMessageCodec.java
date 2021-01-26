package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

@SuppressWarnings("rawtypes")
public class LazyTdResultMessageCodec implements MessageCodec<TdResultMessage, TdResultMessage> {

	private final String codecName;
	private static final TdResultMessageCodec realCodec = new TdResultMessageCodec();

	public LazyTdResultMessageCodec() {
		super();
		this.codecName = "TdResultCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultMessage t) {
		realCodec.encodeToWire(buffer, t);
	}

	@Override
	public TdResultMessage decodeFromWire(int pos, Buffer buffer) {
		return new TdResultMessage(pos, buffer);
	}

	@Override
	public TdResultMessage transform(TdResultMessage t) {
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
