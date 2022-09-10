package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNullElse;
import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class KafkaSharedTdlibServers implements Closeable {

	private final KafkaTdlibServersChannels kafkaTdlibServersChannels;
	private final Disposable responsesSub;
	private final AtomicReference<Disposable> requestsSub = new AtomicReference<>();
	private final Many<OnResponse<TdApi.Object>> responses = Sinks.many().unicast().onBackpressureBuffer(
			Queues.<OnResponse<TdApi.Object>>get(65535).get());
	private final Flux<GroupedFlux<Long, Timestamped<OnRequest<Object>>>> requests;

	public KafkaSharedTdlibServers(KafkaTdlibServersChannels kafkaTdlibServersChannels) {
		this.kafkaTdlibServersChannels = kafkaTdlibServersChannels;
		this.responsesSub = kafkaTdlibServersChannels.response()
				.sendMessages(0L, responses.asFlux())
				.subscribeOn(Schedulers.parallel())
				.subscribe();
		this.requests = kafkaTdlibServersChannels.request()
				.consumeMessages("td-requests")
				.onBackpressureBuffer()
				.groupBy(k -> k.data().userId(), 1)
				.replay()
				.autoConnect(1, this.requestsSub::set);
	}

	public Flux<Timestamped<OnRequest<Object>>> requests(long userId) {
		return requests.filter(group -> group.key() == userId)
				.take(1, true)
				.singleOrEmpty()
				.flatMapMany(Function.identity())
				.log("req-" + userId, Level.FINE, SignalType.REQUEST, SignalType.ON_NEXT);
	}

	public Disposable events(Flux<ClientBoundEvent> eventFlux) {
		return kafkaTdlibServersChannels.events()
				.sendMessages(0L, eventFlux)
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	public Many<OnResponse<TdApi.Object>> responses() {
		return responses;
	}

	@Override
	public void close() {
		responsesSub.dispose();
		var requestsSub = this.requestsSub.get();
		if (requestsSub != null) {
			requestsSub.dispose();
		}
		kafkaTdlibServersChannels.close();
	}
}
