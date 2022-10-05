package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.tdlight.reactiveapi.rsocket.PendingEventsToProduce.ClientPendingEventsToProduce;
import it.tdlight.reactiveapi.rsocket.PendingEventsToProduce.ServerPendingEventsToProduce;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public class MyRSocketClient implements Closeable, RSocketChannelManager, SocketAcceptor {

	private static final Logger LOG = LogManager.getLogger(MyRSocketClient.class);

	private final Empty<Void> closeRequest = Sinks.empty();
	private final AtomicReference<Closeable> clientRef = new AtomicReference<>();
	private final Mono<RSocket> client;
	private final ConcurrentMap<String, PendingEventsToProduce> messagesToProduce = new ConcurrentHashMap<>();

	public MyRSocketClient(HostAndPort baseHost) {
		RetryBackoffSpec retryStrategy = Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(16)).jitter(1.0);
		var transport = TcpClientTransport.create(baseHost.getHost(), baseHost.getPort());

		this.client = RSocketConnector.create()
				.setupPayload(DefaultPayload.create("client", "setup-info"))
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				.acceptor(this)
				.connect(transport)
				.retryWhen(retryStrategy)
				.doOnNext(clientRef::set)
				.cacheInvalidateIf(RSocket::isDisposed)
				.takeUntilOther(closeRequest.asMono())
				.doOnDiscard(RSocket.class, RSocket::dispose);
	}

	@Override
	public <K> EventConsumer<K> registerConsumer(ChannelCodec channelCodec, String channelName) {
		LOG.debug("Registering consumer for channel \"{}\"", channelName);
		Deserializer<K> deserializer;
		try {
			deserializer = channelCodec.getNewDeserializer();
		} catch (Throwable ex) {
			LOG.error("Failed to create codec for channel \"{}\"", channelName, ex);
			throw new IllegalStateException("Failed to create codec for channel \"" + channelName + "\"", ex);
		}
		return new EventConsumer<K>() {
			@Override
			public ChannelCodec getChannelCodec() {
				return channelCodec;
			}

			@Override
			public String getChannelName() {
				return channelName;
			}

			@Override
			public Flux<Timestamped<K>> consumeMessages() {
				return client.flatMapMany(client -> {
					var rawFlux = client.requestStream(DefaultPayload.create(channelName, "notify-can-consume"));
					return rawFlux.map(elementPayload -> {
						var slice = elementPayload.sliceData();
						byte[] elementBytes = new byte[slice.readableBytes()];
						slice.readBytes(elementBytes, 0, elementBytes.length);
						return new Timestamped<>(System.currentTimeMillis(), deserializer.deserialize(null, elementBytes));
					}).log("CLIENT_CONSUME_MESSAGES", Level.FINE);
				});
			}
		};
	}

	@Override
	public <K> EventProducer<K> registerProducer(ChannelCodec channelCodec, String channelName) {
		LOG.debug("Registering producer for channel \"{}\"", channelName);
		Serializer<K> serializer;
		try {
			serializer = channelCodec.getNewSerializer();
		} catch (Throwable ex) {
			LOG.error("Failed to create codec for channel \"{}\"", channelName, ex);
			throw new IllegalStateException("Failed to create codec for channel \"" + channelName + "\"", ex);
		}
		Empty<Void> emitCloseRequest = Sinks.empty();
		return new EventProducer<K>() {
			@Override
			public ChannelCodec getChannelCodec() {
				return channelCodec;
			}

			@Override
			public String getChannelName() {
				return channelName;
			}

			@Override
			public Mono<Void> sendMessages(Flux<K> eventsFlux) {
				return client.flatMap(client -> {
					LOG.debug("Subscribed to channel \"{}\", sending messages to server", channelName);
					var rawFlux = eventsFlux
							.map(event -> DefaultPayload.create(serializer.serialize(null, event)))
							.log("CLIENT_PRODUCE_MESSAGES", Level.FINE)
							.takeUntilOther(emitCloseRequest.asMono().doFinally(s -> LOG.debug("Producer of channel \"{}\" ended the flux because emit close request ended with signal {}", channelName, s)))
							.doFinally(s -> LOG.debug("Producer of channel \"{}\" ended the flux with signal {}", channelName, s))
							.doOnError(ex -> LOG.error("Producer of channel \"{}\" ended the flux with an error", channelName, ex));
					final ServerPendingEventsToProduce myPendingEventsToProduce = new ServerPendingEventsToProduce(rawFlux, new CompletableFuture<>(), new CompletableFuture<>());
					var pendingEventsToProduce = messagesToProduce.computeIfAbsent(channelName, n -> myPendingEventsToProduce);
					if (pendingEventsToProduce instanceof ClientPendingEventsToProduce clientPendingEventsToProduce) {
						if (!clientPendingEventsToProduce.fluxCf().complete(rawFlux)) {
							LOG.error("Called sendMessage twice for channel \"{}\"", channelCodec);
							return Mono.error(new CancelledChannelException("Called sendMessage twice for channel \"" + channelName + "\""));
						}
						return Mono
								.firstWithSignal(client.onClose(), Mono.fromFuture(clientPendingEventsToProduce::doneCf))
								.doOnError(clientPendingEventsToProduce.doneCf()::completeExceptionally)
								.doFinally(s -> {
									messagesToProduce.remove(channelName, clientPendingEventsToProduce);
									clientPendingEventsToProduce.doneCf().complete(null);
									LOG.debug("Producer of channel \"{}\" ended the execution with signal {}", channelName, s);
								});
					} else if (pendingEventsToProduce == myPendingEventsToProduce) {
						return client
								.requestResponse(DefaultPayload.create(channelName, "notify-can-produce"))
								.then(Mono.firstWithSignal(client.onClose(), Mono.fromFuture(myPendingEventsToProduce::doneCf)))
								.doOnError(myPendingEventsToProduce.doneCf()::completeExceptionally)
								.doFinally(s -> {
									messagesToProduce.remove(channelName, myPendingEventsToProduce);
									myPendingEventsToProduce.doneCf().complete(null);
									LOG.debug("Producer of channel \"{}\" ended the execution with signal {}", channelName, s);
								});
					} else {
						LOG.error("Called sendMessage twice for channel \"{}\"", channelCodec);
						return Mono.error(new CancelledChannelException("Called sendMessage twice for channel \"" + channelName + "\""));
					}
				});
			}

			@Override
			public void close() {
				emitCloseRequest.tryEmitEmpty();
			}
		};
	}

	@Override
	public @NotNull Mono<Void> onClose() {
		return closeRequest.asMono().then(Mono.fromSupplier(clientRef::get).flatMap(Closeable::onClose));
	}

	@Override
	public void dispose() {
		var client = clientRef.get();
		if (client != null) {
			client.dispose();
		}
		closeRequest.tryEmitEmpty();
	}

	@Override
	public @NotNull Mono<RSocket> accept(@NotNull ConnectionSetupPayload setup, @NotNull RSocket sendingSocket) {
		return Mono.just(new RSocket() {
			@Override
			public @NotNull Flux<Payload> requestStream(@NotNull Payload payload) {
				return MyRSocketClient.this.requestStream(sendingSocket, payload);
			}
		});
	}

	@NotNull
	private Flux<Payload> requestStream(RSocket sendingSocket, Payload payload) {
		if (payload.getMetadataUtf8().equals("notify-can-consume")) {
			var channel = payload.getDataUtf8();
			LOG.debug("Received request for channel \"{}\", sending stream to server", channel);

			final ClientPendingEventsToProduce myPendingEventsToProduce = new ClientPendingEventsToProduce(new CompletableFuture<>(),
					new CompletableFuture<>(),
					new CompletableFuture<>()
			);
			var pendingEventsToProduce = messagesToProduce.computeIfAbsent(channel, n -> myPendingEventsToProduce);
			if (pendingEventsToProduce instanceof ServerPendingEventsToProduce serverPendingEventsToProduce) {
				if (serverPendingEventsToProduce.initCf().complete(null)) {
					return serverPendingEventsToProduce.events()
							.doOnError(serverPendingEventsToProduce.doneCf()::completeExceptionally)
							.doFinally(s -> {
								messagesToProduce.remove(channel, serverPendingEventsToProduce);
								serverPendingEventsToProduce.doneCf().complete(null);
							});
				} else {
					LOG.error("The channel \"{}\" is already active", channel);
					return Flux.error(new CancelledChannelException("The channel \"" + channel + "\" is already active"));
				}
			} else if (pendingEventsToProduce == myPendingEventsToProduce) {
				if (myPendingEventsToProduce.initCf().complete(null)) {
					return Mono
							.fromFuture(myPendingEventsToProduce::fluxCf)
							.flatMapMany(flux -> flux)
							.doOnError(myPendingEventsToProduce.doneCf()::completeExceptionally)
							.doFinally(s -> myPendingEventsToProduce.doneCf().complete(null));
				} else {
					LOG.error("The channel \"{}\" is already active", channel);
					return Flux.error(new CancelledChannelException("The channel \"" + channel + "\" is already active"));
				}
			} else {
				LOG.error("The channel \"{}\" is already active", channel);
				return Flux.error(new CancelledChannelException("The channel \"" + channel + "\" is already active"));
			}
		} else {
			LOG.warn("Received invalid request stream");
			return Flux.empty();
		}
	}
}
