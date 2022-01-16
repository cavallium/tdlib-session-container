package it.tdlight.reactiveapi;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.MessagingException;
import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.Request;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DynamicAtomixReactiveApiClient implements ReactiveApiClient, AutoCloseable {

	private static final long LIVE_ID_UNSET = -1L;
	private static final long LIVE_ID_FAILED = -2L;

	private final ReactiveApi api;
	private final ClusterEventService eventService;
	private final AtomicLong liveId = new AtomicLong(LIVE_ID_UNSET);
	private final Disposable liveIdSubscription;
	private final long userId;

	private final Flux<ClientBoundEvent> clientBoundEvents;
	private final Flux<Long> liveIdChange;
	private final Mono<Long> liveIdResolution;

	private volatile boolean closed;

	DynamicAtomixReactiveApiClient(AtomixReactiveApi api, KafkaConsumer kafkaConsumer, long userId, String subGroupId) {
		this.api = api;
		this.eventService = api.getAtomix().getEventService();
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
		this.liveIdResolution = this.resolveLiveId();
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}
	
	@Override
	public <T extends TdApi.Object> Mono<T> request(TdApi.Function<T> request, Instant timeout) {
		return liveIdResolution
				.flatMap(liveId -> Mono.fromCompletionStage(() -> eventService.send("session-" + liveId + "-requests",
						new Request<>(liveId, request, timeout),
						LiveAtomixReactiveApiClient::serializeRequest,
						LiveAtomixReactiveApiClient::deserializeResponse,
						Duration.between(Instant.now(), timeout)
				)).subscribeOn(Schedulers.boundedElastic()).onErrorMap(ex -> {
					if (ex instanceof MessagingException.NoRemoteHandler) {
						return new TdError(404, "Bot #IDU" + this.userId + " (liveId: " + liveId + ") is not found on the cluster");
					} else if (ex instanceof TimeoutException) {
						return new TdError(408, "Request Timeout", ex);
					} else {
						return ex;
					}
				}))
				.<T>handle((item, sink) -> {
					if (item instanceof TdApi.Error error) {
						sink.error(new TdError(error.code, error.message));
					} else {
						//noinspection unchecked
						sink.next((T) item);
					}
				});
	}

	private Mono<Long> resolveLiveId() {
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

	@Override
	public long getUserId() {
		return userId;
	}

	@Override
	public boolean isPullMode() {
		return true;
	}

	public Flux<Long> liveIdChange() {
		return liveIdChange;
	}

	public void close() {
		this.closed = true;
		liveIdSubscription.dispose();
	}
}
