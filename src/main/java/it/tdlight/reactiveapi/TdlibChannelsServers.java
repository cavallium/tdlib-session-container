package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.io.Closeable;
import java.util.Map;

public record TdlibChannelsServers(EventConsumer<Event.OnRequest<it.tdlight.jni.TdApi.Object>> request,
																	 EventProducer<Event.OnResponse<it.tdlight.jni.TdApi.Object>> response,
																	 Map<String, EventProducer<ClientBoundEvent>> events) implements Closeable {

	public EventProducer<ClientBoundEvent> events(String lane) {
		var p = events.get(lane);
		if (p == null) {
			throw new IllegalArgumentException("No lane " + lane);
		}
		return p;
	}

	@Override
	public void close() {
		response.close();
		events.values().forEach(EventProducer::close);
	}
}
