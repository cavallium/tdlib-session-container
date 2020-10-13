package it.tdlight.tdlibsession.td.easy;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationState;
import java.util.Objects;
import java.util.StringJoiner;

public class AsyncTdUpdateObj {
	private final AuthorizationState state;
	private final TdApi.Object update;

	public AsyncTdUpdateObj(AuthorizationState state, TdApi.Object update) {
		this.state = state;
		this.update = update;
	}

	public AuthorizationState getState() {
		return state;
	}

	public TdApi.Object getUpdate() {
		return update;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AsyncTdUpdateObj that = (AsyncTdUpdateObj) o;
		return Objects.equals(state, that.state) && Objects.equals(update, that.update);
	}

	@Override
	public int hashCode() {
		return Objects.hash(state, update);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", AsyncTdUpdateObj.class.getSimpleName() + "[", "]")
				.add("state=" + state)
				.add("update=" + update)
				.toString();
	}
}
