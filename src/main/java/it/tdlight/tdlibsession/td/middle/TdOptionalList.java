package it.tdlight.tdlibsession.td.middle;

import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResult;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class TdOptionalList {
	private final boolean isSet;
	private final List<TdResult<TdApi.Object>> values;

	public TdOptionalList(boolean isSet, List<TdResult<TdApi.Object>> values) {
		this.isSet = isSet;
		this.values = values;
	}

	public boolean isSet() {
		return isSet;
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

		TdOptionalList that = (TdOptionalList) o;

		if (isSet != that.isSet) {
			return false;
		}
		return Objects.equals(values, that.values);
	}

	@Override
	public int hashCode() {
		int result = (isSet ? 1 : 0);
		result = 31 * result + (values != null ? values.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TdOptionalList.class.getSimpleName() + "[", "]")
				.add("isSet=" + isSet)
				.add("values=" + values)
				.toString();
	}
}
