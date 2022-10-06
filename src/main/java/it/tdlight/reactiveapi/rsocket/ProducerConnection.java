package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;

class ProducerConnection<T> {

	private Object remote;

	private Flux<Payload> local;

	private Empty<Void> connected = Sinks.empty();

	private Empty<Void> remoteResult = Sinks.empty();

	public ProducerConnection(String channel) {

	}

	public synchronized Mono<Void> connectLocal() {
		return connected.asMono().publishOn(Schedulers.parallel()).then(Mono.defer(() -> {
			synchronized (ProducerConnection.this) {
				return remoteResult.asMono().doFinally(r -> reset());
			}
		})).doOnError(ex -> {
			synchronized (ProducerConnection.this) {
				remoteResult.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			}
		}).doFinally(ended -> reset());
	}

	public synchronized Flux<Payload> connectRemote() {
		return connected.asMono().publishOn(Schedulers.parallel()).thenMany(Flux.defer(() -> {
			synchronized (ProducerConnection.this) {
				return local;
			}
		}).doOnError(ex -> {
			synchronized (ProducerConnection.this) {
				remoteResult.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			}
		})).doFinally(ended -> reset());
	}

	public synchronized void reset() {
		if (local != null && remote != null) {
			remoteResult.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			local = null;
			remote = null;
			connected = Sinks.empty();
			remoteResult = Sinks.empty();
		}
	}

	public synchronized void registerRemote() {
		if (this.remote != null) {
			throw new IllegalStateException("Remote is already registered");
		}
		this.remote = new Object();
		onChanged();
	}

	public synchronized void registerLocal(Flux<Payload> local) {
		if (this.local != null) {
			throw new IllegalStateException("Local is already registered");
		}
		this.local = local;
		onChanged();
	}

	private synchronized void onChanged() {
		if (local != null && remote != null) {
			connected.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
		}
	}
}
