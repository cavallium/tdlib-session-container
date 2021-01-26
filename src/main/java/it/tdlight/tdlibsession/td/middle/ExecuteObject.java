package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import java.util.Objects;
import java.util.StringJoiner;

public class ExecuteObject {

	private static final TdExecuteObjectMessageCodec realCodec = new TdExecuteObjectMessageCodec();

	private boolean executeDirectly;
	private TdApi.Function request;
	private int pos;
	private Buffer buffer;

	public ExecuteObject(boolean executeDirectly, Function request) {
		this.executeDirectly = executeDirectly;
		this.request = request;
		if (request == null) throw new NullPointerException();
	}

	public ExecuteObject(int pos, Buffer buffer) {
		this.pos = pos;
		this.buffer = buffer;
	}

	private void tryDecode() {
		if (request == null) {
			var data = realCodec.decodeFromWire(pos, buffer);
			this.executeDirectly = data.executeDirectly;
			this.request = data.request;
			this.buffer = null;
		}
	}

	public boolean isExecuteDirectly() {
		tryDecode();
		return executeDirectly;
	}

	public TdApi.Function getRequest() {
		tryDecode();
		return request;
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

		ExecuteObject that = (ExecuteObject) o;

		if (executeDirectly != that.executeDirectly) {
			return false;
		}
		return Objects.equals(request, that.request);
	}

	@Override
	public int hashCode() {
		tryDecode();
		int result = (executeDirectly ? 1 : 0);
		result = 31 * result + (request != null ? request.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ExecuteObject.class.getSimpleName() + "[", "]")
				.add("executeDirectly=" + executeDirectly)
				.add("request=" + request)
				.toString();
	}
}
