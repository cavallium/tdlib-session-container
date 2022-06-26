package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import java.io.Closeable;
import java.io.IOException;

public record KafkaTdlibClient(KafkaTdlibRequestProducer request,
															 KafkaTdlibResponseConsumer response,
															 KafkaClientBoundConsumer events) implements Closeable {

	@Override
	public void close() {
		request.close();
	}
}
