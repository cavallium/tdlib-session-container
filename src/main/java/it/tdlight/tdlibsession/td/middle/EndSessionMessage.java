package it.tdlight.tdlibsession.td.middle;

import java.util.Arrays;
import java.util.Objects;

public final class EndSessionMessage {

	private final int id;
	private final byte[] binlog;

	EndSessionMessage(int id, byte[] binlog) {
		this.id = id;
		this.binlog = binlog;
	}

	public int id() {
		return id;
	}

	public byte[] binlog() {
		return binlog;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		var that = (EndSessionMessage) obj;
		return this.id == that.id && Arrays.equals(this.binlog, that.binlog);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, binlog);
	}

	@Override
	public String toString() {
		return "EndSessionMessage[" + "id=" + id + ", " + "binlog=" + Arrays.hashCode(binlog) + ']';
	}
}
