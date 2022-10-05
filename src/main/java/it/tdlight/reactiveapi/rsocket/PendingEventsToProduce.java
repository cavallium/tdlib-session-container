package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
import java.util.concurrent.CompletableFuture;
import reactor.core.publisher.Flux;

sealed interface PendingEventsToProduce {

	record ServerPendingEventsToProduce(Flux<Payload> events, CompletableFuture<Void> initCf,
																			CompletableFuture<Void> doneCf) implements PendingEventsToProduce {}

	record ClientPendingEventsToProduce(CompletableFuture<Void> doneCf,
																			CompletableFuture<Flux<Payload>> fluxCf,
																			CompletableFuture<Void> initCf) implements PendingEventsToProduce {}
}
