package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

@SuppressWarnings("rawtypes")
public class LazyTdResultListMessageCodec implements MessageCodec<TdResultList, TdResultList> {

	private final String codecName;
	private static final TdResultListMessageCodec realCodec = new TdResultListMessageCodec();

	public LazyTdResultListMessageCodec() {
		super();
		this.codecName = "TdOptListCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultList t) {
		realCodec.encodeToWire(buffer, t);
	}

	@Override
	public TdResultList decodeFromWire(int pos, Buffer buffer) {
		return new TdResultList(pos, buffer);
	}

	@Override
	public TdResultList transform(TdResultList t) {
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
