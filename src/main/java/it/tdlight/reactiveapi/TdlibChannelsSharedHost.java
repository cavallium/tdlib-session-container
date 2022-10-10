package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public class TdlibChannelsSharedHost implements Closeable {

	private static final Logger LOG = LogManager.getLogger(TdlibChannelsSharedHost.class);
	public static final RetryBackoffSpec RETRY_STRATEGY = Retry
			.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
			.maxBackoff(Duration.ofSeconds(16))
			.jitter(1.0)
			.doBeforeRetry(signal -> LogManager.getLogger("Channels").warn("Retrying channel with signal {}", signal));

	public static final Function<Flux<Long>, Flux<Long>> REPEAT_STRATEGY = n -> n
			.doOnNext(i -> LogManager.getLogger("Channels").debug("Resubscribing to channel"))
			.delayElements(Duration.ofSeconds(5));

	private final TdlibChannelsServers tdServersChannels;
	private final Disposable responsesSub;
	private final AtomicReference<Disposable> requestsSub = new AtomicReference<>();
	private final Many<OnResponse<TdApi.Object>> responses = Sinks.many().multicast().directAllOrNothing();
	private final Map<String, Many<Flux<ClientBoundEvent>>> events;
	private final Flux<Timestamped<OnRequest<Object>>> requests;

	public TdlibChannelsSharedHost(Set<String> allLanes, TdlibChannelsServers tdServersChannels) {
		this.tdServersChannels = tdServersChannels;
		this.responsesSub = Mono.defer(() -> tdServersChannels.response()
				.sendMessages(responses.asFlux()/*.log("responses", Level.FINE)*/))
				.repeatWhen(REPEAT_STRATEGY)
				.retryWhen(RETRY_STRATEGY)
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> LOG.error("Unexpected error when sending responses", ex));
		events = allLanes.stream().collect(Collectors.toUnmodifiableMap(Function.identity(), lane -> {
			Many<Flux<ClientBoundEvent>> sink = Sinks.many().replay().all();
			Flux<ClientBoundEvent> outputEventsFlux = Flux
					.merge(sink.asFlux().map(flux -> flux.publishOn(Schedulers.parallel())), Integer.MAX_VALUE, 256)
					.doFinally(s -> LOG.debug("Output events flux of lane \"{}\" terminated with signal {}", lane, s));
			Mono.defer(() -> tdServersChannels.events(lane)
					.sendMessages(outputEventsFlux))
					.repeatWhen(REPEAT_STRATEGY)
					.retryWhen(RETRY_STRATEGY)
					.subscribeOn(Schedulers.parallel())
					.subscribe(n -> {}, ex -> LOG.error("Unexpected error when sending events to lane {}", lane, ex));
			return sink;
		}));
		this.requests = tdServersChannels.request().consumeMessages()
				.doFinally(s -> LOG.debug("Input requests consumer terminated with signal {}", s))
				.repeatWhen(REPEAT_STRATEGY)
				.retryWhen(RETRY_STRATEGY)
				.doOnError(ex -> LOG.error("Unexpected error when receiving requests", ex))
				.doFinally(s -> LOG.debug("Input requests flux terminated with signal {}", s));
	}

	public Flux<Timestamped<OnRequest<Object>>> requests() {
		return requests
				//.onBackpressureBuffer(8192, BufferOverflowStrategy.DROP_OLDEST)
				.log("requests", Level.FINEST, SignalType.REQUEST, SignalType.ON_NEXT);
	}

	public Disposable events(String lane, Flux<ClientBoundEvent> eventFlux) {
		Empty<Void> canceller = Sinks.empty();
		var eventsSink = events.get(lane);
		if (eventsSink == null) {
			throw new IllegalArgumentException("Lane " + lane + " does not exist");
		}
		eventsSink.emitNext(eventFlux.takeUntilOther(canceller.asMono()),
				EmitFailureHandler.busyLooping(Duration.ofMillis(100))
		);
		return () -> canceller.tryEmitEmpty();
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
		tdServersChannels.close();
	}
}
