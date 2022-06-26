package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import java.io.Closeable;
import java.io.IOException;

public record KafkaTdlibServer(KafkaTdlibRequestConsumer request,
															 KafkaTdlibResponseProducer response,
															 KafkaClientBoundProducer events) implements Closeable {

	@Override
	public void close() {
		response.close();
		events.close();
	}
}
