package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;
import it.tdlight.utils.VertxBufferInputStream;
import it.tdlight.utils.VertxBufferOutputStream;
import org.warp.commonutils.stream.SafeDataInputStream;
import org.warp.commonutils.stream.SafeDataOutputStream;

public class StartSessionMessageCodec implements MessageCodec<StartSessionMessage, StartSessionMessage> {

	private final String codecName;

	public StartSessionMessageCodec() {
		super();
		this.codecName = "StartSessionMessageCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, StartSessionMessage t) {
		try (var bos = new VertxBufferOutputStream(buffer)) {
			try (var dos = new SafeDataOutputStream(bos)) {
				dos.writeInt(t.id());
				dos.writeUTF(t.alias());
				dos.writeInt(t.binlog().length);
				dos.write(t.binlog());
				dos.writeLong(t.binlogDate());
				dos.writeUTF(t.implementationDetails().toString());
			}
		}
	}

	@Override
	public StartSessionMessage decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos)) {
			try (var dis = new SafeDataInputStream(fis)) {
				return new StartSessionMessage(dis.readInt(),
						dis.readUTF(),
						dis.readNBytes(dis.readInt()),
						dis.readLong(),
						new JsonObject(dis.readUTF())
				);
			}
		}
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
