package it.tdlight.reactiveapi;

import java.io.Closeable;
import java.util.Map;

public record KafkaTdlibClientsChannels(KafkaTdlibRequestProducer request,
																				KafkaTdlibResponseConsumer response,
																				Map<String, KafkaClientBoundConsumer> events) implements Closeable {

	@Override
	public void close() {
		request.close();
	}
}
