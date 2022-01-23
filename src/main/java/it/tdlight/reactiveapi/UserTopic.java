package it.tdlight.reactiveapi;

public class UserTopic {

	private final String value;

	public UserTopic(long userId) {
		value = "tdlib.event.%d".formatted(userId);
	}

	public String getTopic() {
		return value;
	}
}
