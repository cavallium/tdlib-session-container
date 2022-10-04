package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.util.Map;

public record TdlibChannelsClients(EventProducer<OnRequest<?>> request,
																	 EventConsumer<OnResponse<Object>> response,
																	 Map<String, EventConsumer<ClientBoundEvent>> events) implements Closeable {

	@Override
	public void close() {
		request.close();
	}
}
