package it.tdlight.tdlibsession.td.middle;

import it.tdlight.jni.TdApi;
import java.util.Objects;
import java.util.StringJoiner;

public class ExecuteObject {
	private final boolean executeDirectly;
	private final TdApi.Function request;

	public ExecuteObject(boolean executeDirectly, TdApi.Function request) {
		this.executeDirectly = executeDirectly;
		this.request = request;
	}

	public boolean isExecuteDirectly() {
		return executeDirectly;
	}

	public TdApi.Function getRequest() {
		return request;
	}

	@Override
	public boolean equals(Object o) {
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
