package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.utils.BufferUtils;
import java.time.Duration;

public class TdExecuteObjectMessageCodec implements MessageCodec<ExecuteObject, ExecuteObject> {

	public TdExecuteObjectMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, ExecuteObject t) {
		BufferUtils.encode(buffer, os -> {
			os.writeBoolean(t.isExecuteDirectly());
			t.getRequest().serialize(os);
			os.writeLong(t.getTimeout().toMillis());
		});
	}

	@Override
	public ExecuteObject decodeFromWire(int pos, Buffer buffer) {
		return BufferUtils.decode(pos, buffer, is -> new ExecuteObject(
				is.readBoolean(),
				(Function) TdApi.Deserializer.deserialize(is),
				Duration.ofMillis(is.readLong())
		));
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
