package it.tdlight.tdlibsession.td.middle;

import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResult;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class TdResultList {
	private final List<TdResult<TdApi.Object>> values;

	public TdResultList(List<TdResult<TdApi.Object>> values) {
		this.values = values;
	}

	public List<TdResult<TdApi.Object>> getValues() {
		return values;
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

		return Objects.equals(values, that.values);
	}

	@Override
	public int hashCode() {
		return values != null ? values.hashCode() : 0;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TdResultList.class.getSimpleName() + "[", "]").add("values=" + values).toString();
	}
}
