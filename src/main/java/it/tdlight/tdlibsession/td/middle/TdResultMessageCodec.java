package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.utils.BufferUtils;

@SuppressWarnings("rawtypes")
public class TdResultMessageCodec implements MessageCodec<TdResultMessage, TdResultMessage> {

	private final String codecName;

	public TdResultMessageCodec() {
		super();
		this.codecName = "TdResultCodec";
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultMessage t) {
		BufferUtils.encode(buffer, os -> {
			if (t.value != null) {
				os.writeBoolean(true);
				t.value.serialize(os);
			} else {
				os.writeBoolean(false);
				t.cause.serialize(os);
			}
		});
	}

	@Override
	public TdResultMessage decodeFromWire(int pos, Buffer buffer) {
		return BufferUtils.decode(pos, buffer, is -> {
			if (is.readBoolean()) {
				return new TdResultMessage(TdApi.Deserializer.deserialize(is), null);
			} else {
				return new TdResultMessage(null, (TdApi.Error) TdApi.Deserializer.deserialize(is));
			}
		});
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
