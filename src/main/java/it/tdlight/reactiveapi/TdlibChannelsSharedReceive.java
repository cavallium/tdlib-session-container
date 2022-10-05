package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public class TdlibChannelsSharedReceive implements Closeable {

	private static final Logger LOG = LogManager.getLogger(TdlibChannelsSharedReceive.class);

	private static final RetryBackoffSpec RETRY_STRATEGY = Retry
			.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
			.maxBackoff(Duration.ofSeconds(16))
			.jitter(1.0)
			.doBeforeRetry(signal -> LOG.warn("Retrying channel with signal {}", signal));

	private final TdlibChannelsClients tdClientsChannels;
	private final AtomicReference<Disposable> responsesSub = new AtomicReference<>();
	private final Disposable requestsSub;
	private final AtomicReference<Disposable> eventsSub = new AtomicReference<>();
	private final Flux<Timestamped<OnResponse<Object>>> responses;
	private final Map<String, Flux<Timestamped<ClientBoundEvent>>> events;
	private final Many<OnRequest<?>> requests = Sinks.many().multicast().onBackpressureBuffer(65535);

	public TdlibChannelsSharedReceive(TdlibChannelsClients tdClientsChannels) {
		this.tdClientsChannels = tdClientsChannels;
		this.responses = tdClientsChannels
				.response()
				.consumeMessages()
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(5)))
				.retryWhen(RETRY_STRATEGY)
				.doFinally(s -> LOG.debug("Input responses flux terminated with signal {}", s));
		this.events = tdClientsChannels.events().entrySet().stream()
				.collect(Collectors.toUnmodifiableMap(Entry::getKey,
						e -> e
								.getValue()
								.consumeMessages()
								.repeatWhen(n -> n.delayElements(Duration.ofSeconds(5)))
								.retryWhen(RETRY_STRATEGY)
								.doFinally(s -> LOG.debug("Input events flux of lane \"{}\" terminated with signal {}", e.getKey(), s))
				));
		var requestsFlux = Flux.defer(() -> requests.asFlux()
				.doFinally(s -> LOG.debug("Output requests flux terminated with signal {}", s)));
		this.requestsSub = tdClientsChannels.request()
				.sendMessages(requestsFlux)
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(5)))
				.retryWhen(RETRY_STRATEGY)
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> requests.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100))));
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
