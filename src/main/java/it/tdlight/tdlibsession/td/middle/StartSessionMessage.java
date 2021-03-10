package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.buffer.Buffer;
import java.util.Objects;
import java.util.StringJoiner;

public final class StartSessionMessage {

	private final long id;
	private final String alias;
	private final Buffer binlog;
	private final long binlogDate;
	private final JsonObject implementationDetails;

	public StartSessionMessage(long id, String alias, Buffer binlog, long binlogDate, JsonObject implementationDetails) {
		this.id = id;
		this.alias = alias;
		this.binlog = binlog;
		this.binlogDate = binlogDate;
		this.implementationDetails = implementationDetails;
	}

	public long id() {
		return id;
	}

	public String alias() {
		return alias;
	}

	public Buffer binlog() {
		return binlog;
	}

	public long binlogDate() {
		return binlogDate;
	}

	public JsonObject implementationDetails() {
		return implementationDetails;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StartSessionMessage that = (StartSessionMessage) o;
		return id == that.id && binlogDate == that.binlogDate && Objects.equals(alias, that.alias) && Objects.equals(binlog,
				that.binlog
		) && Objects.equals(implementationDetails, that.implementationDetails);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, alias, binlog, binlogDate, implementationDetails);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", StartSessionMessage.class.getSimpleName() + "[", "]")
				.add("id=" + id)
				.add("alias='" + alias + "'")
				.add("binlog=" + binlog)
				.add("binlogDate=" + binlogDate)
				.add("implementationDetails=" + implementationDetails)
				.toString();
	}
}
