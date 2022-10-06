package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
import it.tdlight.reactiveapi.Timestamped;
import java.time.Duration;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;

class ConsumerConnection<T> {

	private Flux<Timestamped<T>> remote;

	private Deserializer<T> local;

	private Empty<Void> connected = Sinks.empty();

	private Empty<Void> remoteResult = Sinks.empty();

	public ConsumerConnection(String channel) {

	}

	public synchronized Flux<Timestamped<T>> connectLocal() {
		return connected.asMono().publishOn(Schedulers.parallel()).thenMany(Flux.defer(() -> {
			synchronized (ConsumerConnection.this) {
				return remote;
			}
		}));
	}

	public synchronized Mono<Void> connectRemote() {
		return connected.asMono().publishOn(Schedulers.parallel()).then(Mono.defer(() -> {
			synchronized (ConsumerConnection.this) {
				return remoteResult.asMono();
			}
		}));
	}

	public synchronized void resetRemote() {
		connected = Sinks.empty();
		remoteResult = Sinks.empty();
		remote = null;
		local = null;
	}

	public synchronized void registerRemote(Flux<Payload> remote) {
		if (this.remote != null) {
			throw new IllegalStateException("Remote is already registered");
		}
		this.remote = remote
				.transformDeferred(flux -> {
					synchronized (ConsumerConnection.this) {
						assert local != null;
						return RSocketUtils.deserialize(flux, local);
					}
				})
				.map(element -> new Timestamped<>(System.currentTimeMillis(), element))
				.doOnError(ex -> {
					synchronized (ConsumerConnection.this) {
						remoteResult.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
					}
				})
				.doFinally(s -> {
					synchronized (ConsumerConnection.this) {
						remoteResult.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
						resetRemote();
					}
				});
		onChanged();
	}

	public synchronized void registerLocal(Deserializer<T> local) {
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
