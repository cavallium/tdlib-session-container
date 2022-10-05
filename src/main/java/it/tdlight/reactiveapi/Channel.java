package it.tdlight.reactiveapi;

public enum Channel {
	CLIENT_BOUND_EVENT("event"),
	TDLIB_REQUEST("request"),
	TDLIB_RESPONSE("response");

	private final String name;

	Channel(String name) {
		this.name = name;
	}

	public String getChannelName() {
		return name;
	}

	@Override
	public String toString() {
		return name;
	}
}
