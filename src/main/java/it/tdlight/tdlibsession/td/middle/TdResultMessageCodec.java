package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@SuppressWarnings("rawtypes")
public class TdResultMessageCodec implements MessageCodec<TdResultMessage, TdResultMessage> {

	private final String codecName;

	public TdResultMessageCodec() {
		super();
		this.codecName = "TdResultCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultMessage t) {
		try (var bos = new FastByteArrayOutputStream()) {
			try (var dos = new DataOutputStream(bos)) {
				if (t.value != null) {
					dos.writeBoolean(true);
					t.value.serialize(dos);
				} else {
					dos.writeBoolean(false);
					t.cause.serialize(dos);
				}
			}
			bos.trim();
			buffer.appendBytes(bos.array);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public TdResultMessage decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new FastByteArrayInputStream(buffer.getBytes(pos, buffer.length()))) {
			try (var dis = new DataInputStream(fis)) {
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
