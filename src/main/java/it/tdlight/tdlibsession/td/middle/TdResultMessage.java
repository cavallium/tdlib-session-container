package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.TdResult;
import java.util.StringJoiner;

public class TdResultMessage {

	private static final TdResultMessageCodec realCodec = new TdResultMessageCodec();

	public TdApi.Object value;
	public TdApi.Error cause;
	private int pos;
	private Buffer buffer;

	public TdResultMessage(Object value, Error cause) {
		this.value = value;
		this.cause = cause;
		if (value == null && cause == null) throw new NullPointerException("Null message");
	}

	public TdResultMessage(int pos, Buffer buffer) {
		this.pos = pos;
		this.buffer = buffer;
	}

	public <T extends Object> TdResult<T> toTdResult() {
		if (value != null) {
			//noinspection unchecked
			return TdResult.succeeded((T) value);
		} else if (cause != null) {
			return TdResult.failed(cause);
		} else {
			var data = realCodec.decodeFromWire(pos, buffer);
			this.value = data.value;
			this.cause = data.cause;
			this.buffer = null;
			if (value != null) {
				//noinspection unchecked
				return TdResult.succeeded((T) value);
			} else {
				return TdResult.failed(cause);
			}
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TdResultMessage.class.getSimpleName() + "[", "]")
				.add("value=" + value)
				.add("cause=" + cause)
				.toString();
	}
}
