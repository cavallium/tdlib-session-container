package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class TdResultList {

	private static final TdResultListMessageCodec realCodec = new TdResultListMessageCodec();

	private List<TdApi.Object> values;
	private Error error;
	private int pos;
	private Buffer buffer;

	public TdResultList(List<TdApi.Object> values) {
		this.values = values;
		this.error = null;
		if (values == null) throw new NullPointerException("Null message");
	}

	public TdResultList(TdApi.Error error) {
		this.values = null;
		this.error = error;
		if (error == null) throw new NullPointerException("Null message");
	}

	public TdResultList(int pos, Buffer buffer) {
		this.pos = pos;
		this.buffer = buffer;
	}

	private void tryDecode() {
		if (error == null && values == null) {
			var value = realCodec.decodeFromWire(pos, buffer);
			this.values = value.values;
			this.error = value.error;
			this.buffer = null;
		}
	}

	public List<TdApi.Object> value() {
		tryDecode();
		return values;
	}

	public TdApi.Error error() {
		tryDecode();
		return error;
	}

	public boolean succeeded() {
		tryDecode();
		return error == null && values != null;
	}

	@Override
	public boolean equals(Object o) {
		tryDecode();
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TdResultList that = (TdResultList) o;

		if (!Objects.equals(values, that.values)) {
			return false;
		}
		return Objects.equals(error, that.error);
	}

	@Override
	public int hashCode() {
		tryDecode();
		int result = values != null ? values.hashCode() : 0;
		result = 31 * result + (error != null ? error.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TdResultList.class.getSimpleName() + "[", "]")
				.add("values=" + values)
				.add("error=" + error)
				.toString();
	}
}
