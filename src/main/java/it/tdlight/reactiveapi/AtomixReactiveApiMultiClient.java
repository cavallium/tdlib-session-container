package it.tdlight.reactiveapi;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.cluster.messaging.Subscription;
import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.Request;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;

public class AtomixReactiveApiMultiClient implements ReactiveApiMultiClient, AutoCloseable {

	private final ReactiveApi api;
	private final ClusterEventService eventService;

	private final Flux<ClientBoundEvent> clientBoundEvents;

	AtomixReactiveApiMultiClient(AtomixReactiveApi api) {
		this.api = api;
		this.eventService = api.getAtomix().getEventService();

		clientBoundEvents = Flux
				.<ClientBoundEvent>push(sink -> {
					var subscriptionFuture = eventService.subscribe("session-client-bound-events",
							LiveAtomixReactiveApiClient::deserializeEvent,
							s -> {
								sink.next(s);
								return CompletableFuture.completedFuture(null);
							},
							(a) -> null
					);
					sink.onDispose(() -> subscriptionFuture.thenAccept(Subscription::close));
				}, OverflowStrategy.ERROR)
				.onBackpressureBuffer(0xFFFF, BufferOverflowStrategy.ERROR)
				.share();
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}

	@Override
	public <T extends TdApi.Object> Mono<T> request(long userId, long liveId, TdApi.Function<T> request, Instant timeout) {
		return Mono.fromCompletionStage(() -> eventService.send("session-" + liveId + "-requests",
						new Request<>(liveId, request, timeout),
						LiveAtomixReactiveApiClient::serializeRequest,
						LiveAtomixReactiveApiClient::deserializeResponse,
						Duration.between(Instant.now(), timeout)
				))
				.<T>handle((item, sink) -> {
					if (item instanceof TdApi.Error error) {
						sink.error(new TdError(error.code, error.message));
					} else {
						//noinspection unchecked
						sink.next((T) item);
					}
				})
				.onErrorMap(ex -> {
					if (ex instanceof MessagingException.NoRemoteHandler) {
						return new TdError(404, "Bot #IDU" + userId + " (live id: " + liveId + ") is not found on the cluster");
					} else {
						return ex;
					}
				});
	}

	@Override
	public void close() {
	}
}
