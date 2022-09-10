package it.tdlight.reactiveapi;

import java.io.Closeable;

public record KafkaTdlibClientsChannels(KafkaTdlibRequestProducer request,
																				KafkaTdlibResponseConsumer response,
																				KafkaClientBoundConsumer events) implements Closeable {

	@Override
	public void close() {
		request.close();
	}
}
