package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.AuthorizationStateClosing;
import it.tdlight.jni.TdApi.AuthorizationStateLoggingOut;
import it.tdlight.jni.TdApi.AuthorizationStateReady;
import it.tdlight.jni.TdApi.Close;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.jni.TdApi.UpdateNewMessage;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

public class PeriodicRestarter {

	private static final Logger LOG = LoggerFactory.getLogger(PeriodicRestarter.class);

	private static Duration JITTER_MAX_DELAY = Duration.ofMinutes(5);

	private final ReactiveApi api;
	private final Duration interval;
	private final Set<Long> restartUserIds;
	private final ReactiveApiMultiClient multiClient;

	/**
	 * Live id -> x
	 */
	private final ConcurrentMap<Long, Disposable> closeManagedByPeriodicRestarter = new ConcurrentHashMap<>();

	/**
	 * Live id -> x
	 */
	private final ConcurrentMap<Long, Boolean> closingByPeriodicRestarter = new ConcurrentHashMap<>();

	/**
	 * Live id -> x
	 */
	private final ConcurrentMap<Long, Boolean> sessionAuthReady = new ConcurrentHashMap<>();

	public PeriodicRestarter(ReactiveApi api, Duration interval, Set<Long> restartUserIds) {
		this.api = api;
		this.interval = interval;
		this.restartUserIds = restartUserIds;

		this.multiClient = api.multiClient("periodic-restarter");

	}

	public Mono<Void> start() {
		return Mono.fromRunnable(() -> {
			LOG.info("Starting periodic restarter...");
			multiClient
					.clientBoundEvents(true)

					// Filter events to reduce overhead
					.filter(event -> {
						boolean isAllowedUpdate;
						if (event instanceof OnUpdateData onUpdateData) {
							isAllowedUpdate = switch (onUpdateData.update().getConstructor()) {
								case UpdateAuthorizationState.CONSTRUCTOR, UpdateNewMessage.CONSTRUCTOR -> true;
								default -> false;
							};
						} else {
							isAllowedUpdate = false;
						}
						return isAllowedUpdate && restartUserIds.contains(event.userId());
					})
					.cast(OnUpdateData.class)

					.doOnNext(event -> {
						if (event.update() instanceof UpdateAuthorizationState updateAuthorizationState) {
							if (updateAuthorizationState.authorizationState instanceof AuthorizationStateReady) {
								// Session is now ready
								this.sessionAuthReady.put(event.liveId(), true);
								onSessionReady(event.liveId(), event.userId());
							} else if (updateAuthorizationState.authorizationState instanceof AuthorizationStateLoggingOut) {
								// Session is not ready anymore
								this.sessionAuthReady.remove(event.liveId(), false);
							} else if (updateAuthorizationState.authorizationState instanceof AuthorizationStateClosing) {
								// Session is not ready anymore
								this.sessionAuthReady.remove(event.liveId(), false);
							} else if (updateAuthorizationState.authorizationState instanceof AuthorizationStateClosed) {
								// Session is not ready anymore
								this.sessionAuthReady.remove(event.liveId(), false);
								Boolean prev = closingByPeriodicRestarter.remove(event.liveId());
								var disposable = closeManagedByPeriodicRestarter.remove(event.userId());
								boolean managed = prev != null && prev;
								// Check if the live session is managed by the periodic restarter
								if (managed) {
									LOG.info("The session #IDU{} (liveId: {}) is being started", event.userId(), event.liveId());
									// Restart the session
									api.tryReviveSession(event.userId()).subscribeOn(Schedulers.parallel()).subscribe();
								}
								// Dispose restarter anyway
								if (disposable != null && !disposable.isDisposed()) {
									disposable.dispose();
								}
							}
						} else if (event.update() instanceof UpdateNewMessage) {
							var wasReady = requireNonNullElse(this.sessionAuthReady.put(event.liveId(), true), false);
							if (!wasReady) {
								onSessionReady(event.liveId(), event.userId());
							}
						}
					})
					.subscribeOn(Schedulers.parallel())
					.subscribe();
			LOG.info("Started periodic restarter");
		});
	}

	private void onSessionReady(long liveId, long userId) {
		Duration maxRandomDelay;
		if (JITTER_MAX_DELAY.compareTo(interval) < 0) {
			maxRandomDelay = JITTER_MAX_DELAY;
		} else {
			maxRandomDelay = interval;
		}
		var randomDelay = randomTime(maxRandomDelay);

		var totalDelay = interval.plus(randomDelay);

		LOG.info("The session #IDU{} (liveId: {}) will be restarted at {}",
				userId,
				liveId,
				Instant.now().plus(totalDelay)
		);

		// Restart after x time
		AtomicReference<Disposable> disposableRef = new AtomicReference<>();
		var disposable = Schedulers
				.parallel()
				.schedule(() -> {
					closeManagedByPeriodicRestarter.remove(liveId, disposableRef.get());
					LOG.info("The session #IDU{} (liveId: {}) is being stopped", userId, liveId);
					if (!requireNonNullElse(closingByPeriodicRestarter.put(liveId, true), false)) {
						// Request restart
						multiClient
								.request(userId, liveId, new Close(), Instant.now().plus(Duration.ofMinutes(5)))
								.subscribeOn(Schedulers.parallel())
								.retryWhen(Retry.backoff(5, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(5)))
								.doOnError(ex -> {
									if (ex instanceof TdError tdError && tdError.getTdCode() == 404) {
										LOG.warn("Failed to restart bot {} (liveId={}): not found", userId, liveId);
									} else {
										LOG.error("Failed to restart bot {} (liveId={})", userId, liveId, ex);
									}
								})
								.onErrorResume(ex -> Mono.empty())
								.subscribe();
					}

				}, totalDelay.toMillis(), TimeUnit.MILLISECONDS);
		disposableRef.set(disposable);
		var prev = closeManagedByPeriodicRestarter.put(liveId, disposable);
		if (prev != null) prev.dispose();
	}

	/**
	 * @return random duration from 0 to n
	 */
	private Duration randomTime(Duration max) {
		return Duration.ofMillis(ThreadLocalRandom.current().nextInt(0, Math.toIntExact(max.toMillis())));
	}

	public Mono<Void> stop() {
		return Mono.fromRunnable(() -> {
			LOG.info("Stopping periodic restarter...");
			multiClient.close();
			LOG.info("Stopped periodic restarter");
		});
	}
}
