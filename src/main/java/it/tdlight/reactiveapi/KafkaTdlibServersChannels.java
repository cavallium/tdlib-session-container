package it.tdlight.reactiveapi;

import java.io.Closeable;
import java.util.Map;

public record KafkaTdlibServersChannels(KafkaTdlibRequestConsumer request,
																				KafkaTdlibResponseProducer response,
																				Map<String, KafkaClientBoundProducer> events) implements Closeable {

	public KafkaClientBoundProducer events(String lane) {
		var p = events.get(lane);
		if (p == null) {
			throw new IllegalArgumentException("No lane " + lane);
		}
		return p;
	}

	@Override
	public void close() {
		response.close();
		events.values().forEach(KafkaProducer::close);
	}
}
