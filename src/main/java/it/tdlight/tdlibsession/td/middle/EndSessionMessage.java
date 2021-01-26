package it.tdlight.tdlibsession.td.middle;

import io.vertx.reactivex.core.buffer.Buffer;
import java.util.Objects;
import java.util.StringJoiner;

public final class EndSessionMessage {

	private final int id;
	private final Buffer binlog;

	public EndSessionMessage(int id, Buffer binlog) {
		this.id = id;
		this.binlog = binlog;
	}

	public int id() {
		return id;
	}

	public Buffer binlog() {
		return binlog;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		EndSessionMessage that = (EndSessionMessage) o;
		return id == that.id && Objects.equals(binlog, that.binlog);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, binlog);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", EndSessionMessage.class.getSimpleName() + "[", "]")
				.add("id=" + id)
				.add("binlog=" + binlog)
				.toString();
	}
}
