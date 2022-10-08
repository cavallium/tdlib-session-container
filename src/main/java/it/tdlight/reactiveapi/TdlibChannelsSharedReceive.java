package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.TdlibChannelsSharedHost.REPEAT_STRATEGY;
import static it.tdlight.reactiveapi.TdlibChannelsSharedHost.RETRY_STRATEGY;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
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

	private final TdlibChannelsClients tdClientsChannels;
	private final AtomicReference<Disposable> responsesSub = new AtomicReference<>();
	private final Disposable requestsSub;
	private final AtomicReference<Disposable> eventsSub = new AtomicReference<>();
	private final Flux<Timestamped<OnResponse<Object>>> responses;
	private final Map<String, Flux<Timestamped<ClientBoundEvent>>> events;
	private final Many<OnRequest<?>> requests = Sinks.many().multicast().directAllOrNothing();

	public TdlibChannelsSharedReceive(TdlibChannelsClients tdClientsChannels) {
		this.tdClientsChannels = tdClientsChannels;
		this.responses = Flux
				.defer(() -> tdClientsChannels.response().consumeMessages())
				//.log("responses", Level.FINE)
				.repeatWhen(REPEAT_STRATEGY)
				.retryWhen(RETRY_STRATEGY)
				.doFinally(s -> LOG.debug("Input responses flux terminated with signal {}", s));
		this.events = tdClientsChannels.events().entrySet().stream()
				.collect(Collectors.toUnmodifiableMap(Entry::getKey,
						e -> Flux
								.defer(() -> e.getValue().consumeMessages())
								.repeatWhen(REPEAT_STRATEGY)
								.retryWhen(RETRY_STRATEGY)
								.doFinally(s -> LOG.debug("Input events flux of lane \"{}\" terminated with signal {}", e.getKey(), s))
				));
		this.requestsSub = tdClientsChannels
				.request()
				.sendMessages(Flux.defer(() -> requests.asFlux().doFinally(s -> LOG.debug("Output requests flux terminated with signal {}", s))))
				.doFinally(s -> LOG.debug("Output requests sender terminated with signal {}", s))
				.repeatWhen(REPEAT_STRATEGY)
				.retryWhen(RETRY_STRATEGY)
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> {
					LOG.error("An error when handling requests killed the requests subscriber!", ex);
					synchronized (requests) {
						requests.emitError(ex, EmitFailureHandler.FAIL_FAST);
					}
				});
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

	public void emitRequest(OnRequest<?> request) {
		synchronized (requests) {
			requests.emitNext(request, EmitFailureHandler.FAIL_FAST);
		}
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
