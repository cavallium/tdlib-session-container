package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;
import it.tdlight.utils.BufferUtils;
import org.warp.commonutils.serialization.UTFUtils;

public class StartSessionMessageCodec implements MessageCodec<StartSessionMessage, StartSessionMessage> {

	private final String codecName;

	public StartSessionMessageCodec() {
		super();
		this.codecName = "StartSessionMessageCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, StartSessionMessage t) {
		BufferUtils.encode(buffer, os -> {
			os.writeInt(t.id());
			UTFUtils.writeUTF(os, t.alias());
			BufferUtils.writeBuf(os, t.binlog());
			os.writeLong(t.binlogDate());
			UTFUtils.writeUTF(os, t.implementationDetails().toString());
		});
	}

	@Override
	public StartSessionMessage decodeFromWire(int pos, Buffer buffer) {
		return BufferUtils.decode(pos, buffer, is -> new StartSessionMessage(is.readInt(),
				UTFUtils.readUTF(is),
				BufferUtils.rxReadBuf(is),
				is.readLong(),
				new JsonObject(UTFUtils.readUTF(is))
		));
	}

	@Override
	public StartSessionMessage transform(StartSessionMessage t) {
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
