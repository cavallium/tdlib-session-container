package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public final class StartSessionMessage {

	private final int id;
	private final String alias;
	private final byte[] binlog;
	private final long binlogDate;
	private final JsonObject implementationDetails;

	public StartSessionMessage(int id, String alias, byte[] binlog, long binlogDate, JsonObject implementationDetails) {
		this.id = id;
		this.alias = alias;
		this.binlog = binlog;
		this.binlogDate = binlogDate;
		this.implementationDetails = implementationDetails;
	}

	public int id() {
		return id;
	}

	public String alias() {
		return alias;
	}

	public byte[] binlog() {
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

		if (id != that.id) {
			return false;
		}
		if (binlogDate != that.binlogDate) {
			return false;
		}
		if (!Objects.equals(alias, that.alias)) {
			return false;
		}
		if (!Arrays.equals(binlog, that.binlog)) {
			return false;
		}
		return Objects.equals(implementationDetails, that.implementationDetails);
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + (alias != null ? alias.hashCode() : 0);
		result = 31 * result + Arrays.hashCode(binlog);
		result = 31 * result + (int) (binlogDate ^ (binlogDate >>> 32));
		result = 31 * result + (implementationDetails != null ? implementationDetails.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", StartSessionMessage.class.getSimpleName() + "[", "]")
				.add("id=" + id)
				.add("alias='" + alias + "'")
				.add("binlog=" + Arrays.toString(binlog))
				.add("binlogDate=" + binlogDate)
				.add("implementationDetails=" + implementationDetails)
				.toString();
	}
}
