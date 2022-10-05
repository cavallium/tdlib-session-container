package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class ClientsSharedTdlib implements Closeable {

	private static final Logger LOG = LogManager.getLogger(ClientsSharedTdlib.class);

	private final TdlibChannelsClients tdClientsChannels;
	private final AtomicReference<Disposable> responsesSub = new AtomicReference<>();
	private final Disposable requestsSub;
	private final AtomicReference<Disposable> eventsSub = new AtomicReference<>();
	private final Flux<Timestamped<OnResponse<Object>>> responses;
	private final Map<String, Flux<Timestamped<ClientBoundEvent>>> events;
	private final Many<OnRequest<?>> requests = Sinks.many().unicast()
			.onBackpressureBuffer(Queues.<OnRequest<?>>get(65535).get());

	public ClientsSharedTdlib(TdlibChannelsClients tdClientsChannels) {
		this.tdClientsChannels = tdClientsChannels;
		this.responses = tdClientsChannels.response().consumeMessages();
		this.events = tdClientsChannels.events().entrySet().stream()
				.collect(Collectors.toUnmodifiableMap(Entry::getKey, e -> e.getValue().consumeMessages()));
		this.requestsSub = tdClientsChannels.request()
				.sendMessages(requests.asFlux())
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	public Flux<Timestamped<OnResponse<Object>>> responses() {
		return responses;
	}

	public Flux<Timestamped<ClientBoundEvent>> events(String lane) {
		var result = events.get(lane);
		if (result == null) {
			throw new IllegalArgumentException("No lane " + lane);
		}
		return result;
	}

	public Map<String, Flux<Timestamped<ClientBoundEvent>>> events() {
		return events;
	}

	public Many<OnRequest<?>> requests() {
		return requests;
	}

	@Override
	public void close() {
		requestsSub.dispose();
		var responsesSub = this.responsesSub.get();
		if (responsesSub != null) {
			responsesSub.dispose();
		}
		var eventsSub = this.eventsSub.get();
		if (eventsSub != null) {
			eventsSub.dispose();
		}
		tdClientsChannels.close();
	}
}
