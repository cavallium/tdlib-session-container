package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
import it.tdlight.reactiveapi.Deserializer;
import it.tdlight.reactiveapi.Timestamped;
import java.time.Duration;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class ConsumerConnection<T> {

	private static final Logger LOG = LogManager.getLogger(ConsumerConnection.class);

	private final String channel;
	private final int bufferSize;
	private Many<Flux<Payload>> remotes = Sinks.many().replay().all();
	private int remoteCount = 0;

	private Deserializer<T> local;

	private boolean connectedState = false;
	private Empty<Void> connectedSink = Sinks.empty();
	private Optional<Throwable> localTerminationState = null;
	private Empty<Void> localTerminationSink = Sinks.empty();

	public ConsumerConnection(String channel, int bufferSize) {
		this.channel = channel;
		this.bufferSize = bufferSize;
		if (LOG.isDebugEnabled()) LOG.debug("{} Create new blank connection", this.printStatus());
	}

	private String printStatus() {
		return "[\"%s\" (%d)%s%s%s]".formatted(channel,
				System.identityHashCode(this),
				local != null ? ", local" : "",
				remoteCount > 0 ? (remoteCount > 1 ? ", " + remoteCount + " remotes" : ", 1 remote") : "",
				connectedState ? ((localTerminationState != null) ? (localTerminationState.isPresent() ? ", done with error" : ", done") : ", connected") : ", waiting"
		);
	}

	public synchronized Flux<Timestamped<T>> connectLocal() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Local is asking to connect", this.printStatus());
		return Mono.defer(() -> {
			synchronized (ConsumerConnection.this) {
				return connectedSink.asMono();
			}
		}).publishOn(Schedulers.parallel()).thenMany(Flux.defer(() -> {
			synchronized (ConsumerConnection.this) {
				if (LOG.isDebugEnabled()) LOG.debug("{} Local is connected", this.printStatus());
				return Flux.merge(remotes.asFlux().map(remote -> {
							return remote.doOnError(ex -> {
								synchronized (ConsumerConnection.this) {
									if (remoteCount <= 1) {
										if (remoteCount > 0 && localTerminationState == null) {
											localTerminationState = Optional.of(ex);
											if (LOG.isDebugEnabled()) {
												LOG.debug("%s Local connection ended with failure".formatted(this.printStatus()), ex);
											}
											if (remoteCount <= 1) {
												var sink = localTerminationSink;
												reset();
												sink.emitError(ex, EmitFailureHandler.FAIL_FAST);
												if (LOG.isDebugEnabled()) {
													LOG.debug("%s Local connection ended with failure, emitted termination failure".formatted(this.printStatus()));
												}
											}
										}
									} else {
										remoteCount--;
										if (LOG.isDebugEnabled()) {
											LOG.debug("%s Local connection ended with failure, but at least one remote is still online".formatted(
													this.printStatus()));
										}
									}
								}
							}).doFinally(s -> {
								if (s != SignalType.ON_ERROR) {
									synchronized (ConsumerConnection.this) {
										if (LOG.isDebugEnabled()) LOG.debug("{} Remote connection ending with status {}", this.printStatus(), s);
										if (remoteCount <= 1) {
											if (remoteCount > 0 && localTerminationState == null) {
												assert connectedState;
												localTerminationState = Optional.empty();
												if (s == SignalType.CANCEL) {
													localTerminationSink.emitError(new CancelledChannelException(), EmitFailureHandler.FAIL_FAST);
												} else {
													localTerminationSink.emitEmpty(EmitFailureHandler.FAIL_FAST);
												}
											}
											reset();
											if (LOG.isDebugEnabled()) LOG.debug("{} Remote connection ended with status {}, emitted termination complete", this.printStatus(), s);
										} else {
											remoteCount--;
											if (LOG.isDebugEnabled()) LOG.debug("{} Remote connection ended with status {}, but at least one remote is still online", this.printStatus(), s);
										}
									}
								}
							}).onErrorResume(ex -> {
								synchronized (ConsumerConnection.this) {
									if (remoteCount <= 1) {
										return Flux.error(ex);
									} else {
										return Flux.empty();
									}
								}
							});
						}), Integer.MAX_VALUE, bufferSize)
						.transform(remote -> {
							synchronized (ConsumerConnection.this) {
								return RSocketUtils.deserialize(remote, local);
							}
						})
						.map(element -> new Timestamped<>(System.currentTimeMillis(), element));
			}
		})).doFinally(s -> {
			if (s == SignalType.CANCEL) {
				synchronized (ConsumerConnection.this) {
					local = null;
					var ex = new InterruptedException();
					localTerminationState = Optional.of(ex);
					if (LOG.isDebugEnabled()) LOG.debug("{} Local is cancelled", this.printStatus());
					localTerminationSink.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
				}
			}
		});
	}

	public synchronized Mono<Void> connectRemote() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote is asking to connect", this.printStatus());
		return Mono.defer(() -> {
			synchronized (ConsumerConnection.this) {
				return connectedSink.asMono();
			}
		}).publishOn(Schedulers.parallel()).then(Mono.defer(() -> {
			synchronized (ConsumerConnection.this) {
				if (LOG.isDebugEnabled()) LOG.debug("{} Remote is connected", this.printStatus());
				return localTerminationSink.asMono().publishOn(Schedulers.parallel());
			}
		}));
	}

	public synchronized void reset() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Reset started", this.printStatus());
		if (connectedState) {
			if (localTerminationState == null) {
				if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection is still marked as open but not terminated, interrupting it", this.printStatus());
				var ex = new InterruptedException();
				localTerminationState = Optional.of(ex);
				localTerminationSink.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
				if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection has been interrupted", this.printStatus());
			}
		} else {
			if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection is still marked as waiting for a connection, interrupting it", this.printStatus());
			localTerminationState = Optional.empty();
			localTerminationSink.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection has been interrupted", this.printStatus());
		}
		local = null;
		remoteCount = 0;
		remotes.emitComplete(EmitFailureHandler.FAIL_FAST);
		remotes = Sinks.many().replay().all();
		connectedState = false;
		connectedSink = Sinks.empty();
		localTerminationState = null;
		localTerminationSink = Sinks.empty();
		if (LOG.isDebugEnabled()) LOG.debug("{} Reset ended", this.printStatus());
	}

	public synchronized void registerRemote(Flux<Payload> remote) {
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote is trying to register", this.printStatus());
		this.remoteCount++;
		this.remotes.emitNext(remote, EmitFailureHandler.FAIL_FAST);
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote registered", this.printStatus());
		onChanged();
	}

	public synchronized Throwable registerLocal(Deserializer<T> local) {
		if (LOG.isDebugEnabled()) LOG.debug("{} Local is trying to register", this.printStatus());
		if (this.local != null) {
			if (LOG.isDebugEnabled()) LOG.debug("{} Local was already registered", this.printStatus());
			return new IllegalStateException("Local is already registered");
		}
		this.local = local;
		if (LOG.isDebugEnabled()) LOG.debug("{} Local registered", this.printStatus());
		onChanged();
		return null;
	}

	private synchronized void onChanged() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Checking connection changes", this.printStatus());
		if (local != null && remoteCount > 0) {
			connectedState = true;
			if (LOG.isDebugEnabled()) LOG.debug("{} Connected successfully! Emitting connected event", this.printStatus());
			connectedSink.emitEmpty(EmitFailureHandler.FAIL_FAST);
			if (LOG.isDebugEnabled()) LOG.debug("{} Connected successfully! Emitted connected event", this.printStatus());
		} else {
			if (LOG.isDebugEnabled()) LOG.debug("{} Still not connected", this.printStatus());
		}
	}
}
