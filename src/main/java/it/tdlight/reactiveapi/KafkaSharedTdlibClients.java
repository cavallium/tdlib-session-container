package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNullElse;
import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

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
		this.responses = kafkaTdlibClientsChannels.response().consumeMessages("td-responses")
				.publish(65535)
				.autoConnect(1, this.responsesSub::set);
		this.events = kafkaTdlibClientsChannels.events().consumeMessages("td-handler")
				.publish(65535)
				.autoConnect(1, this.eventsSub::set);
		this.requestsSub = kafkaTdlibClientsChannels.request()
				.sendMessages(0L, requests.asFlux())
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	public Flux<Timestamped<OnResponse<Object>>> responses(long clientId) {
		return responses
				.filter(group -> group.data().clientId() == clientId)
				//.onBackpressureBuffer(8192, BufferOverflowStrategy.DROP_OLDEST)
				.log("req-" + clientId, Level.FINEST, SignalType.REQUEST);
	}

	public Flux<Timestamped<ClientBoundEvent>> events(long userId) {
		return events
				.filter(group -> group.data().userId() == userId)
				//.onBackpressureBuffer(8192, BufferOverflowStrategy.DROP_OLDEST)
				.doOnSubscribe(s -> LOG.info("Reading updates of client: {}", userId))
				.log("event-" + userId, Level.FINEST, SignalType.REQUEST);
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

	public boolean canRequestsWait() {
		return false;
	}
}
