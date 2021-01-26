package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.utils.BufferUtils;

public class EndSessionMessageCodec implements MessageCodec<EndSessionMessage, EndSessionMessage> {

	private final String codecName;

	public EndSessionMessageCodec() {
		super();
		this.codecName = "EndSessionMessageCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, EndSessionMessage t) {
		BufferUtils.encode(buffer, os -> {
			os.writeInt(t.id());
			BufferUtils.writeBuf(os, t.binlog());
		});
	}

	@Override
	public EndSessionMessage decodeFromWire(int pos, Buffer buffer) {
		return BufferUtils.decode(pos, buffer, is -> new EndSessionMessage(is.readInt(), BufferUtils.rxReadBuf(is)));
	}

	@Override
	public EndSessionMessage transform(EndSessionMessage t) {
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
