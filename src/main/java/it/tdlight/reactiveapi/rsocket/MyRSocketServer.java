package it.tdlight.reactiveapi.rsocket;

import static reactor.util.concurrent.Queues.XS_BUFFER_SIZE;

import com.google.common.net.HostAndPort;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.Deserializer;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Serializer;
import it.tdlight.reactiveapi.Timestamped;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class MyRSocketServer implements RSocketChannelManager, RSocket {

	private final Logger logger = LogManager.getLogger(this.getClass());

	private final int bufferSize;
	private final Mono<CloseableChannel> serverCloseable;

	protected final Map<String, ConsumerConnection<?>> consumerRegistry = new ConcurrentHashMap<>();

	protected final Map<String, ProducerConnection<?>> producerRegistry = new ConcurrentHashMap<>();

	public MyRSocketServer(HostAndPort baseHost) {
		this(baseHost, XS_BUFFER_SIZE);
	}

	public MyRSocketServer(HostAndPort baseHost, int bufferSize) {
		this.bufferSize = bufferSize;
		var serverMono = RSocketServer
				.create(new SocketAcceptor() {
					@Override
					public @NotNull Mono<RSocket> accept(@NotNull ConnectionSetupPayload setup, @NotNull RSocket sendingSocket) {
						if (setup.getMetadataUtf8().equals("setup-info") && setup.getDataUtf8().equals("client")) {
							return Mono.just(MyRSocketServer.this);
						} else {
							return Mono.error(new IOException("Invalid credentials"));
						}
					}
				})
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				.bind(TcpServerTransport.create(baseHost.getHost(), baseHost.getPort()))
				.doOnNext(d -> logger.debug("Server up"))
				.cacheInvalidateIf(CloseableChannel::isDisposed);

		serverMono.subscribeOn(Schedulers.parallel()).subscribe(v -> {}, ex -> logger.warn("Failed to bind server"));

		this.serverCloseable = serverMono;
	}

	@Override
	public @NotNull Flux<Payload> requestChannel(@NotNull Publisher<Payload> payloads) {
		return Flux.from(payloads).switchOnFirst((first, flux) -> {
			if (first.isOnNext()) {
				var firstValue = first.get();
				assert firstValue != null;
				var meta = firstValue.getMetadataUtf8();
				if (!meta.equals("channel")) {
					return Mono.error(new CancelledChannelException("Metadata is wrong"));
				}
				var channel = firstValue.getDataUtf8();
				var conn = MyRSocketServer.this.consumerRegistry.computeIfAbsent(channel,
						ch -> new ConsumerConnection<>(ch, bufferSize));
				conn.registerRemote(flux.skip(1));
				return conn.connectRemote().then(Mono.fromSupplier(() -> DefaultPayload.create("ok", "result")));
			} else {
				return flux.take(1, true);
			}
		});
	}

	@Override
	public @NotNull Flux<Payload> requestStream(@NotNull Payload payload) {
		var channel = payload.getDataUtf8();
		return Flux.defer(() -> {
			var conn = MyRSocketServer.this.producerRegistry.computeIfAbsent(channel,
					ch -> new ProducerConnection<>(ch, bufferSize));
			conn.registerRemote();
			return conn.connectRemote();
		});
	}

	@Override
	public final <K> EventConsumer<K> registerConsumer(ChannelCodec channelCodec, String channelName) {
		logger.debug("Registering consumer for channel \"{}\"", channelName);
		Deserializer<K> deserializer;
		try {
			deserializer = channelCodec.getNewDeserializer();
		} catch (Throwable ex) {
			logger.error("Failed to create codec for channel \"{}\"", channelName, ex);
			throw new IllegalStateException("Failed to create codec for channel " + channelName);
		}
		return new EventConsumer<K>() {
			@Override
			public Flux<Timestamped<K>> consumeMessages() {
				return serverCloseable.flatMapMany(x -> {
					//noinspection unchecked
					var conn = (ConsumerConnection<K>) consumerRegistry.computeIfAbsent(channelName,
							ch -> new ConsumerConnection<>(ch, bufferSize));
					Throwable ex = conn.registerLocal(deserializer);
					if (ex != null) {
						return Flux.error(ex);
					}
					return conn.connectLocal();
				});
			}
		};
	}

	@Override
	public <K> EventProducer<K> registerProducer(ChannelCodec channelCodec, String channelName) {
		logger.debug("Registering producer for channel \"{}\"", channelName);
		Serializer<K> serializer;
		try {
			serializer = channelCodec.getNewSerializer();
		} catch (Throwable ex) {
			logger.error("Failed to create codec for channel \"{}\"", channelName, ex);
			throw new IllegalStateException("Failed to create codec for channel " + channelName);
		}
		return new EventProducer<>() {
			@Override
			public Mono<Void> sendMessages(Flux<K> eventsFlux) {
				return serverCloseable.flatMap(x -> {
					//noinspection unchecked
					var conn = (ProducerConnection<K>) producerRegistry.computeIfAbsent(channelName,
							ch -> new ProducerConnection<>(ch, bufferSize));
					conn.registerLocal(eventsFlux.transform(flux -> RSocketUtils.serialize(flux, serializer)));
					return conn.connectLocal();
				});
			}

			@Override
			public void close() {

			}
		};
	}

	@Override
	public @NotNull Mono<Void> onClose() {
		return Mono.when(serverCloseable.flatMap(CloseableChannel::onClose));
	}

	@Override
	public void dispose() {
		serverCloseable
				.doOnNext(CloseableChannel::dispose)
				.subscribeOn(Schedulers.parallel())
				.subscribe(v -> {}, ex -> logger.error("Failed to dispose the server", ex));
	}
}
