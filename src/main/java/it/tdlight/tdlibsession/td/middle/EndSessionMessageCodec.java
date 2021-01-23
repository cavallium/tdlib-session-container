package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.utils.VertxBufferInputStream;
import it.tdlight.utils.VertxBufferOutputStream;
import org.warp.commonutils.stream.SafeDataInputStream;
import org.warp.commonutils.stream.SafeDataOutputStream;

public class EndSessionMessageCodec implements MessageCodec<EndSessionMessage, EndSessionMessage> {

	private final String codecName;

	public EndSessionMessageCodec() {
		super();
		this.codecName = "EndSessionMessageCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, EndSessionMessage t) {
		try (var bos = new VertxBufferOutputStream(buffer)) {
			try (var dos = new SafeDataOutputStream(bos)) {
				dos.writeInt(t.id());
				dos.writeInt(t.binlog().length);
				dos.write(t.binlog());
			}
		}
	}

	@Override
	public EndSessionMessage decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos)) {
			try (var dis = new SafeDataInputStream(fis)) {
				return new EndSessionMessage(dis.readInt(), dis.readNBytes(dis.readInt()));
			}
		}
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
