package it.tdlight.reactiveapi;

import java.util.Objects;

public class UserTopic {

	private final String value;

	public UserTopic(KafkaChannelName channelName, long userId) {
		value = "tdlib.%s.%d".formatted(channelName.getKafkaName(), userId);
	}

	public String getTopic() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		UserTopic userTopic = (UserTopic) o;

		return Objects.equals(value, userTopic.value);
	}
}
