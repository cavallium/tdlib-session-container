package it.tdlight.reactiveapi;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class PeriodicRestarter {

	private static final Logger LOG = LoggerFactory.getLogger(PeriodicRestarter.class);

	private final ReactiveApi api;
	private final Duration interval;
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

	public PeriodicRestarter(ReactiveApi api, Duration interval) {
		this.api = api;
		this.interval = interval;

		this.multiClient = api.multiClient();

	}

	public Mono<Void> start() {
		return Mono.fromRunnable(() -> {
			LOG.info("Starting periodic restarter...");
			multiClient.clientBoundEvents().doOnNext(event -> {
				if (event instanceof OnUpdateData onUpdate) {
					if (onUpdate.update() instanceof UpdateAuthorizationState updateAuthorizationState) {
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
					} else if (onUpdate.update() instanceof UpdateNewMessage) {
						var wasReady = this.sessionAuthReady.getOrDefault(event.liveId(), false);
						if (!wasReady) {
							this.sessionAuthReady.put(event.liveId(), true);
							onSessionReady(event.liveId(), event.userId());
						}
					}
				}
			}).subscribeOn(Schedulers.parallel()).subscribe();
			LOG.info("Started periodic restarter");
		});
	}

	private void onSessionReady(long liveId, long userId) {
		LOG.info("The session #IDU{} (liveId: {}) will be restarted at {}",
				userId,
				liveId,
				Instant.now().plus(interval)
		);

		// Restart after x time
		var disposable = Schedulers
				.parallel()
				.schedule(() -> {
					LOG.info("The session #IDU{} (liveId: {}) is being stopped", userId, liveId);
					closingByPeriodicRestarter.put(liveId, true);
					// Request restart
					multiClient
							.request(userId, liveId, new Close(), Instant.now().plus(Duration.ofSeconds(15)))
							.subscribeOn(Schedulers.parallel())
							.subscribe();

				}, interval.toMillis(), TimeUnit.MILLISECONDS);
		closeManagedByPeriodicRestarter.put(liveId, disposable);
	}

	public Mono<Void> stop() {
		return Mono.fromRunnable(() -> {
			LOG.info("Stopping periodic restarter...");
			multiClient.close();
			LOG.info("Stopped periodic restarter");
		});
	}
}
