package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DynamicAtomixReactiveApiClient extends BaseAtomixReactiveApiClient implements AutoCloseable {

	private static final long LIVE_ID_UNSET = -1L;
	private static final long LIVE_ID_FAILED = -2L;

	private final ReactiveApi api;
	private final AtomicLong liveId = new AtomicLong(LIVE_ID_UNSET);
	private final Disposable liveIdSubscription;
	private final long userId;

	private final Flux<ClientBoundEvent> clientBoundEvents;
	private final Flux<Long> liveIdChange;

	private volatile boolean closed;

	DynamicAtomixReactiveApiClient(AtomixReactiveApi api, KafkaConsumer kafkaConsumer, long userId, String subGroupId) {
		super(api.getAtomix(), userId);
		this.api = api;
		this.userId = userId;

		clientBoundEvents = kafkaConsumer.consumeMessages(subGroupId, userId)
				.doOnNext(e -> liveId.set(e.liveId()))
				.takeWhile(n -> !closed)
				.share();

		liveIdChange = this.clientBoundEvents()
				.sample(Duration.ofSeconds(1))
				.map(Event::liveId)
				.distinctUntilChanged();

		this.liveIdSubscription = liveIdChange.subscribeOn(Schedulers.parallel()).subscribe(liveId::set);
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

	@Override
	protected Mono<Long> resolveLiveId() {
		return Mono
				.fromSupplier(this.liveId::get)
				.flatMap(liveId -> {
					if (liveId == LIVE_ID_UNSET) {
						return api.resolveUserLiveId(userId)
								.switchIfEmpty(Mono.error(this::createLiveIdFailed))
								.doOnError(ex -> this.liveId.compareAndSet(LIVE_ID_UNSET, LIVE_ID_FAILED));
					} else if (liveId == LIVE_ID_FAILED) {
						return Mono.error(createLiveIdFailed());
					} else {
						return Mono.just(liveId);
					}
				});
	}

	private Throwable createLiveIdFailed() {
		return new TdError(404, "Bot #IDU" + this.userId
				+ " is not found on the cluster, no live id has been associated with it locally");
	}

	public Flux<Long> liveIdChange() {
		return liveIdChange;
	}

	public void close() {
		this.closed = true;
		liveIdSubscription.dispose();
	}
}
