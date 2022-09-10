package it.tdlight.reactiveapi;

import java.io.Closeable;

public record KafkaTdlibServersChannels(KafkaTdlibRequestConsumer request,
																				KafkaTdlibResponseProducer response,
																				KafkaClientBoundProducer events) implements Closeable {

	@Override
	public void close() {
		response.close();
		events.close();
	}
}
