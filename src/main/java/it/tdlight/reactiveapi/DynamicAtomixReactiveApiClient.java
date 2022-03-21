package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DynamicAtomixReactiveApiClient extends BaseAtomixReactiveApiClient implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(DynamicAtomixReactiveApiClient.class);

	private record CurrentLiveId(long sinceTimestamp, long liveId) implements Comparable<CurrentLiveId> {

		@Override
		public int compareTo(@NotNull DynamicAtomixReactiveApiClient.CurrentLiveId o) {
			return Long.compare(this.sinceTimestamp, o.sinceTimestamp);
		}
	}

	private final ReactiveApi api;
	private final AtomicReference<Disposable> clientBoundEventsSubscription = new AtomicReference<>(null);
	private final long userId;

	private final Flux<TimestampedClientBoundEvent> clientBoundEvents;
	private final Flux<CurrentLiveId> liveIdChange;

	private volatile boolean closed;

	DynamicAtomixReactiveApiClient(AtomixReactiveApi api, KafkaConsumer kafkaConsumer, long userId, String subGroupId) {
		super(api.getAtomix(), userId);
		this.api = api;
		this.userId = userId;

		var clientBoundEvents = kafkaConsumer
				.consumeMessages(subGroupId, userId)
				.takeWhile(n -> !closed)
				.publish()
				.autoConnect(3, clientBoundEventsSubscription::set)
				.onErrorResume(CancellationException.class, ex -> {
					if ("Disconnected".equals(ex.getMessage())) {
						LOG.debug("Disconnected client {}", userId, ex);
						return Mono.empty();
					} else {
						return Mono.error(ex);
					}
				});

		var firstLiveId = clientBoundEvents
				.take(1, true)
				.singleOrEmpty()
				.map(e -> new CurrentLiveId(e.timestamp(), e.event().liveId()));
		var sampledLiveIds = clientBoundEvents
				.skip(1)
				.sample(Duration.ofSeconds(1))
				.map(e -> new CurrentLiveId(e.timestamp(), e.event().liveId()));
		var startupLiveId = api
				.resolveUserLiveId(userId)
				.doOnError(ex -> LOG.error("Failed to resolve live id of user {}", userId, ex))
				.onErrorResume(ex -> Mono.empty())
				.map(liveId -> new CurrentLiveId(System.currentTimeMillis(), liveId));

		liveIdChange = startupLiveId
				.concatWith(Flux.merge(firstLiveId, sampledLiveIds))
				.scan((prev, next) -> {
					if (next.compareTo(prev) > 0) {
						LOG.trace("Replaced id {} with id {}", prev, next);
						return next;
					} else {
						return prev;
					}
				})
				.distinctUntilChanged(CurrentLiveId::liveId);

		// minimum 3 subscribers:
		//  - firstClientBoundEvent
		//  - sampledClientBoundEvents
		//  - clientBoundEvents
		this.clientBoundEvents = clientBoundEvents;

		super.initialize();
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents.doFirst(() -> {
			if (this.clientBoundEventsSubscription.get() != null) {
				throw new UnsupportedOperationException("Already subscribed");
			}
		}).map(TimestampedClientBoundEvent::event);
	}

	@Override
	protected Flux<Long> liveIdChange() {
		return liveIdChange.map(CurrentLiveId::liveId);
	}

	public void close() {
		this.closed = true;
		var clientBoundEventsSubscription = this.clientBoundEventsSubscription.get();
		if (clientBoundEventsSubscription != null && !clientBoundEventsSubscription.isDisposed()) {
			try {
				clientBoundEventsSubscription.dispose();
			} catch (CancellationException ignored) {
				LOG.debug("Reactive api client for user {} has been cancelled", userId);
			}
		}
		super.close();
	}
}
