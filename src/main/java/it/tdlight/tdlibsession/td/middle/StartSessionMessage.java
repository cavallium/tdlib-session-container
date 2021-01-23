package it.tdlight.tdlibsession.td.middle;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public final class StartSessionMessage {

	private final int id;
	private final String alias;
	private final byte[] binlog;
	private final long binlogDate;

	public StartSessionMessage(int id, String alias, byte[] binlog, long binlogDate) {
		this.id = id;
		this.alias = alias;
		this.binlog = binlog;
		this.binlogDate = binlogDate;
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
		return Arrays.equals(binlog, that.binlog);
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + (alias != null ? alias.hashCode() : 0);
		result = 31 * result + Arrays.hashCode(binlog);
		result = 31 * result + (int) (binlogDate ^ (binlogDate >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", StartSessionMessage.class.getSimpleName() + "[", "]")
				.add("id=" + id)
				.add("alias='" + alias + "'")
				.add("binlog=" + Arrays.toString(binlog))
				.add("binlogDate=" + binlogDate)
				.toString();
	}
}
