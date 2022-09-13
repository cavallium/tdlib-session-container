package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNullElse;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class KafkaSharedTdlibClients implements Closeable {

	private static final Logger LOG = LogManager.getLogger(KafkaSharedTdlibClients.class);

	private final KafkaTdlibClientsChannels kafkaTdlibClientsChannels;
	private final AtomicReference<Disposable> responsesSub = new AtomicReference<>();
	private final Disposable requestsSub;
	private final AtomicReference<Disposable> eventsSub = new AtomicReference<>();
	private final Flux<Timestamped<OnResponse<Object>>> responses;
	private final Flux<Timestamped<ClientBoundEvent>> events;
	private final Many<OnRequest<?>> requests = Sinks.many().unicast()
			.onBackpressureBuffer(Queues.<OnRequest<?>>get(65535).get());

	public KafkaSharedTdlibClients(KafkaTdlibClientsChannels kafkaTdlibClientsChannels) {
		this.kafkaTdlibClientsChannels = kafkaTdlibClientsChannels;
		this.responses = kafkaTdlibClientsChannels.response().consumeMessages("td-responses");
		this.events = kafkaTdlibClientsChannels.events().consumeMessages("td-handler");
		this.requestsSub = kafkaTdlibClientsChannels.request()
				.sendMessages(0L, requests.asFlux())
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	public Flux<Timestamped<OnResponse<Object>>> responses() {
		return responses;
	}

	public Flux<Timestamped<ClientBoundEvent>> events() {
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
		kafkaTdlibClientsChannels.close();
	}
}
