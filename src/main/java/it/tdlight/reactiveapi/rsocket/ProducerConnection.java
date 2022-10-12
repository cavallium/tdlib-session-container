package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
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

public class ProducerConnection<T> {

	private static final Logger LOG = LogManager.getLogger(ProducerConnection.class);

	private final String channel;
	private final int bufferSize;
	private int remoteCount = 0;

	private Flux<Payload> local;

	private boolean connectedState = false;
	private Empty<Void> connectedSink = Sinks.empty();
	private Optional<Throwable> remoteTerminationState = null;
	private Empty<Void> remoteTerminationSink = Sinks.empty();

	public ProducerConnection(String channel, int bufferSize) {
		this.channel = channel;
		this.bufferSize = bufferSize;
		if  (LOG.isDebugEnabled()) LOG.debug("{} Create new blank connection", this.printStatus());
	}

	private synchronized String printStatus() {
		return "[\"%s\" (%d)%s%s%s]".formatted(channel,
				System.identityHashCode(this),
				local != null ? ", local" : "",
				remoteCount > 0 ? (remoteCount > 1 ? ", " + remoteCount + " remotes" : ", 1 remote") : "",
				connectedState ? ((remoteTerminationState != null) ? (remoteTerminationState.isPresent() ? ", done with error" : ", done") : ", connected") : ", waiting"
		);
	}

	public synchronized Mono<Void> connectLocal() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Local is asking to connect", this.printStatus());
		return Mono.defer(() -> {
			synchronized (ProducerConnection.this) {
				return connectedSink.asMono();
			}
		}).publishOn(Schedulers.parallel()).then(Mono.defer(() -> {
			synchronized (ProducerConnection.this) {
				if (LOG.isDebugEnabled()) LOG.debug("{} Local is connected", this.printStatus());
				return remoteTerminationSink.asMono().publishOn(Schedulers.parallel());
			}
		})).doFinally(s -> {
			if (s == SignalType.CANCEL) {
				synchronized (ProducerConnection.this) {
					local = null;
					if (LOG.isDebugEnabled()) LOG.debug("{} Local is cancelled", this.printStatus());
				}
			}
		});
	}

	public synchronized Flux<Payload> connectRemote() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote is asking to connect", this.printStatus());
		return Mono.defer(() -> {
			synchronized (ProducerConnection.this) {
				return connectedSink.asMono();
			}
		}).publishOn(Schedulers.parallel()).thenMany(Flux.defer(() -> {
			synchronized (ProducerConnection.this) {
				if (LOG.isDebugEnabled()) LOG.debug("{} Remote is connected", this.printStatus());
				return local;
			}
		})).doOnError(ex -> {
			synchronized (ProducerConnection.this) {
				if (remoteCount <= 1) {
					if (local != null && remoteTerminationState == null) {
						remoteTerminationState = Optional.of(ex);
						if (LOG.isDebugEnabled()) LOG.debug("%s Remote connection ended with failure, emitting termination failure".formatted(this.printStatus()), ex);
						var sink = remoteTerminationSink;
						reset();
						sink.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
						if (LOG.isDebugEnabled()) LOG.debug("%s Remote connection ended with failure, emitted termination failure".formatted(this.printStatus()));
					}
				} else {
					remoteCount--;
					if (LOG.isDebugEnabled()) {
						LOG.debug("%s Remote connection ended with failure, but at least one remote is still online".formatted(
								this.printStatus()));
					}
				}
			}
		}).doFinally(s -> {
			if (s != SignalType.ON_ERROR) {
				synchronized (ProducerConnection.this) {
					if (LOG.isDebugEnabled()) LOG.debug("{} Remote connection ending with status {}", this.printStatus(), s);
					if (remoteCount <= 1) {
						if (local != null && remoteTerminationState == null) {
							assert connectedState;
							remoteTerminationState = Optional.empty();
							if (LOG.isDebugEnabled()) LOG.debug("{} Remote connection ended with status {}, emitting termination complete", this.printStatus(), s);
							if (s == SignalType.CANCEL) {
								remoteTerminationSink.emitError(new CancelledChannelException(), EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
							} else {
								remoteTerminationSink.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
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
		});
	}

	public synchronized void reset() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Reset started", this.printStatus());
		if (connectedState) {
			if (remoteTerminationState == null) {
				if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection is still marked as open but not terminated, interrupting it", this.printStatus());
				var ex = new InterruptedException();
				remoteTerminationState = Optional.of(ex);
				remoteTerminationSink.emitError(ex, EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
				if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection has been interrupted", this.printStatus());
			}
		} else {
			if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection is still marked as waiting for a connection, interrupting it", this.printStatus());
			remoteTerminationState = Optional.empty();
			remoteTerminationSink.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			if (LOG.isDebugEnabled()) LOG.debug("{} The previous connection has been interrupted", this.printStatus());
		}
		local = null;
		remoteCount = 0;
		connectedState = false;
		connectedSink = Sinks.empty();
		remoteTerminationState = null;
		remoteTerminationSink = Sinks.empty();
		if (LOG.isDebugEnabled()) LOG.debug("{} Reset ended", this.printStatus());
	}

	public synchronized void registerRemote() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote is trying to register", this.printStatus());
		if (this.remoteCount  > 0) {
			if (LOG.isDebugEnabled()) LOG.debug("{} Remote was already registered", this.printStatus());
			throw new IllegalStateException("Remote is already registered");
		}
		this.remoteCount++;
		if (LOG.isDebugEnabled()) LOG.debug("{} Remote registered", this.printStatus());
		onChanged();
	}

	public synchronized void registerLocal(Flux<Payload> local) {
		if (LOG.isDebugEnabled()) LOG.debug("{} Local is trying to register", this.printStatus());
		if (this.local != null) {
			if (LOG.isDebugEnabled()) LOG.debug("{} Local was already registered", this.printStatus());
			throw new IllegalStateException("Local is already registered");
		}
		this.local = local.publish(bufferSize).refCount(1);
		if (LOG.isDebugEnabled()) LOG.debug("{} Local registered", this.printStatus());
		onChanged();
	}

	private synchronized void onChanged() {
		if (LOG.isDebugEnabled()) LOG.debug("{} Checking connection changes", this.printStatus());
		if (local != null && remoteCount > 0) {
			connectedState = true;
			if (LOG.isDebugEnabled()) LOG.debug("{} Connected successfully! Emitting connected event", this.printStatus());
			connectedSink.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			if (LOG.isDebugEnabled()) LOG.debug("{} Connected successfully! Emitted connected event", this.printStatus());
		} else {
			if (LOG.isDebugEnabled()) LOG.debug("{} Still not connected", this.printStatus());
		}
	}
}
