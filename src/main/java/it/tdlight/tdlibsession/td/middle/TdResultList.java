package it.tdlight.tdlibsession.td.middle;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class TdResultList {
	private final List<TdApi.Object> values;
	private final Error error;

	public TdResultList(List<TdApi.Object> values) {
		this.values = values;
		this.error = null;
	}

	public TdResultList(TdApi.Error error) {
		this.values = null;
		this.error = error;
	}

	public List<TdApi.Object> value() {
		return values;
	}

	public TdApi.Error error() {
		return error;
	}

	public boolean succeeded() {
		return error == null && values != null;
	}

	@Override
	public boolean equals(Object o) {
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
