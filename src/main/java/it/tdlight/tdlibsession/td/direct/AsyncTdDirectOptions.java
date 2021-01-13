package it.tdlight.tdlibsession.td.direct;

import java.time.Duration;
import java.util.StringJoiner;

public class AsyncTdDirectOptions {

	private final Duration receiveDuration;
	private final int eventsSize;

	/**
	 *
	 * @param receiveDuration Maximum number of seconds allowed for this function to wait for new records. Default: 1 sec
	 * @param eventsSize Maximum number of events allowed in list. Default: 350 events
	 */
	public AsyncTdDirectOptions(Duration receiveDuration, int eventsSize) {
		this.receiveDuration = receiveDuration;
		this.eventsSize = eventsSize;
	}

	public Duration getReceiveDuration() {
		return receiveDuration;
	}

	public int getEventsSize() {
		return eventsSize;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", AsyncTdDirectOptions.class.getSimpleName() + "[", "]")
				.add("receiveDuration=" + receiveDuration)
				.add("eventsSize=" + eventsSize)
				.toString();
	}
}
