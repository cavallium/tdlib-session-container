package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.utils.VertxBufferInputStream;
import it.tdlight.utils.VertxBufferOutputStream;
import java.io.IOException;
import org.warp.commonutils.stream.SafeDataInputStream;
import org.warp.commonutils.stream.SafeDataOutputStream;

@SuppressWarnings("rawtypes")
public class TdResultMessageCodec implements MessageCodec<TdResultMessage, TdResultMessage> {

	private final String codecName;

	public TdResultMessageCodec() {
		super();
		this.codecName = "TdResultCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultMessage t) {
		try (var bos = new VertxBufferOutputStream(buffer)) {
			try (var dos = new SafeDataOutputStream(bos)) {
				if (t.value != null) {
					dos.writeBoolean(true);
					t.value.serialize(dos.asDataOutputStream());
				} else {
					dos.writeBoolean(false);
					t.cause.serialize(dos.asDataOutputStream());
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public TdResultMessage decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos)) {
			try (var dis = new SafeDataInputStream(fis)) {
				if (dis.readBoolean()) {
					return new TdResultMessage(TdApi.Deserializer.deserialize(dis), null);
				} else {
					return new TdResultMessage(null, (TdApi.Error) TdApi.Deserializer.deserialize(dis));
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return null;
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
