package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.rsocket.Closeable;
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
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.tdlight.reactiveapi.rsocket.MyRSocketServer.PendingEventsToConsume.ClientPendingEventsToConsume;
import it.tdlight.reactiveapi.rsocket.MyRSocketServer.PendingEventsToConsume.ServerPendingEventsToConsume;
import it.tdlight.reactiveapi.rsocket.PendingEventsToProduce.ClientPendingEventsToProduce;
import it.tdlight.reactiveapi.rsocket.PendingEventsToProduce.ServerPendingEventsToProduce;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.logging.Level;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MyRSocketServer implements Closeable, RSocketChannelManager, SocketAcceptor {

	private static final Logger LOG = LogManager.getLogger(MyRSocketServer.class);

	private final Mono<CloseableChannel> server;
	private final Map<String, EventConsumer<?>> consumers = new CopyOnWriteMap<>();
	private final Map<String, EventProducer<?>> producers = new CopyOnWriteMap<>();

	private final ConcurrentMap<String, PendingEventsToConsume> messagesToConsume
			= new ConcurrentHashMap<>();

	sealed interface PendingEventsToConsume {
		record ClientPendingEventsToConsume(Flux<Payload> doneCf,
																				CompletableFuture<Void> initCf) implements PendingEventsToConsume {}
		record ServerPendingEventsToConsume(CompletableFuture<Flux<Payload>> doneCf,
																				CompletableFuture<Void> initCf) implements PendingEventsToConsume {}
	}

	private final ConcurrentMap<String, PendingEventsToProduce> messagesToProduce = new ConcurrentHashMap<>();

	public MyRSocketServer(HostAndPort baseHost) {
		this.server = RSocketServer
				.create(this)
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				.bind(TcpServerTransport.create(baseHost.getHost(), baseHost.getPort()))
				.cache();
	}

	@Override
	public <K> EventConsumer<K> registerConsumer(ChannelCodec channelCodec, String channelName) {
		LOG.debug("Registering consumer for channel \"{}\"", channelName);
		var consumer = new EventConsumer<K>() {
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
				Deserializer<K> deserializer;
				try {
					deserializer = channelCodec.getNewDeserializer();
				} catch (Throwable ex) {
					LOG.error("Failed to create codec for channel \"{}\"", channelName, ex);
					return Flux.error(new IllegalStateException("Failed to create codec for channel " + channelName));
				}
				return Flux.defer(() -> {
					var myPendingEventsToConsume = new ServerPendingEventsToConsume(new CompletableFuture<>(), new CompletableFuture<>());
					var pendingEventsToConsume = messagesToConsume.computeIfAbsent(channelName, n -> myPendingEventsToConsume);
					if (pendingEventsToConsume instanceof ClientPendingEventsToConsume clientPendingEventsToConsume) {
						if (clientPendingEventsToConsume.initCf.complete(null)) {
							return server.thenMany(clientPendingEventsToConsume.doneCf
									.map(elementPayload -> {
										var slice = elementPayload.sliceData();
										byte[] elementBytes = new byte[slice.readableBytes()];
										slice.readBytes(elementBytes, 0, elementBytes.length);
										return new Timestamped<>(System.currentTimeMillis(), deserializer.deserialize(null, elementBytes));
									})
									.log("SERVER_CONSUME_MESSAGES", Level.FINE)
									.doFinally(s -> messagesToConsume.remove(channelName, clientPendingEventsToConsume)));
						} else {
							LOG.error("Channel is already consuming");
							return Mono.error(new CancelledChannelException("Channel is already consuming"));
						}
					} else if (pendingEventsToConsume == myPendingEventsToConsume) {
						return server.thenMany(Mono
								.fromFuture(myPendingEventsToConsume::doneCf)
								.flatMapMany(Function.identity())
								.map(elementPayload -> {
									var slice = elementPayload.sliceData();
									byte[] elementBytes = new byte[slice.readableBytes()];
									slice.readBytes(elementBytes, 0, elementBytes.length);
									return new Timestamped<>(System.currentTimeMillis(), deserializer.deserialize(null, elementBytes));
								})
								.doFinally(s -> messagesToConsume.remove(channelName, myPendingEventsToConsume)));
					} else {
						LOG.error("Channel is already consuming");
						return Mono.error(new CancelledChannelException("Channel is already consuming"));
					}
				});
			}
		};
		var prev = this.consumers.put(channelName, consumer);
		if (prev != null) {
			LOG.error("Consumer \"{}\" was already registered", channelName);
		}
		return consumer;
	}

	@Override
	public <K> EventProducer<K> registerProducer(ChannelCodec channelCodec, String channelName) {
		LOG.debug("Registering producer for channel \"{}\"", channelName);
		Serializer<K> serializer;
		try {
			serializer = channelCodec.getNewSerializer();
		} catch (Throwable ex) {
			LOG.error("Failed to create codec for channel \"{}\"", channelName, ex);
			throw new UnsupportedOperationException("Failed to create codec for channel \"" + channelName + "\"", ex);
		}
		var producer = new EventProducer<K>() {
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
				var serverCloseEvent = server
						.flatMap(CloseableChannel::onClose)
						.doOnSuccess(s ->
								LOG.debug("Channel \"{}\" messages send flux will end because the server is closed", channelName));
				return Mono.defer(() -> {
					var rawFlux = eventsFlux
							.log("SERVER_PRODUCE_MESSAGES", Level.FINE)
							.map(element -> DefaultPayload.create(serializer.serialize(null, element)));
					final ServerPendingEventsToProduce myPendingEventsToProduce = new ServerPendingEventsToProduce(rawFlux, new CompletableFuture<>(), new CompletableFuture<>());
					var pendingEventsToProduce = messagesToProduce.computeIfAbsent(channelName, n -> myPendingEventsToProduce);
					if (pendingEventsToProduce instanceof ClientPendingEventsToProduce clientPendingEventsToProduce) {
						if (clientPendingEventsToProduce.fluxCf().complete(rawFlux)) {
							return Mono
									.firstWithSignal(Mono.fromFuture(clientPendingEventsToProduce::doneCf), serverCloseEvent)
									.doFinally(s -> {
										messagesToProduce.remove(channelName, clientPendingEventsToProduce);
										clientPendingEventsToProduce.doneCf().complete(null);
									});
						} else {
							LOG.error("Called sendMessage twice for channel \"{}\"", channelCodec);
							return Mono.error(new CancelledChannelException("Called sendMessage twice for channel \"" + channelName + "\""));
						}
					} else if (pendingEventsToProduce == myPendingEventsToProduce) {
						return Mono.firstWithSignal(Mono.fromFuture(myPendingEventsToProduce::doneCf), serverCloseEvent)
								.doFinally(s -> {
									messagesToProduce.remove(channelName, myPendingEventsToProduce);
									myPendingEventsToProduce.doneCf().complete(null);
								});
					} else {
						LOG.error("Called sendMessage twice for channel \"{}\"", channelCodec);
						return Mono.error(new CancelledChannelException("Called sendMessage twice for channel \"" + channelName + "\""));
					}
				});
			}

			@Override
			public void close() {

			}
		};
		var prev = this.producers.put(channelName, producer);
		if (prev != null) {
			LOG.error("Producer \"{}\" was already registered", channelName);
			prev.close();
		}
		return producer;
	}

	@Override
	public @NotNull Mono<Void> onClose() {
		return server.flatMap(CloseableChannel::onClose);
	}

	@Override
	public void dispose() {
		server.doOnNext(CloseableChannel::dispose).subscribe(n -> {}, ex -> LOG.error("Failed to dispose the server", ex));
	}

	@Override
	public @NotNull Mono<RSocket> accept(@NotNull ConnectionSetupPayload setup, @NotNull RSocket sendingSocket) {
		if (!setup.getMetadataUtf8().equals("setup-info") || !setup.getDataUtf8().equals("client")) {
			LOG.warn("Invalid setup metadata!");
			return Mono.just(new RSocket() {});
		}
		return Mono.just(new RSocket() {
			@Override
			public @NotNull Flux<Payload> requestStream(@NotNull Payload payload) {
				return MyRSocketServer.this.requestStream(sendingSocket, payload);
			}

			@Override
			public @NotNull Mono<Payload> requestResponse(@NotNull Payload payload) {
				return MyRSocketServer.this.requestResponse(sendingSocket, payload);
			}
		});
	}

	private Mono<Payload> requestResponse(RSocket sendingSocket, Payload payload) {
		if (payload.getMetadataUtf8().equals("notify-can-produce")) {
			var channel = payload.getDataUtf8();
			var consumer = consumers.get(channel);
			if (consumer != null) {
				var rawFlux = sendingSocket.requestStream(DefaultPayload.create(channel, "notify-can-consume"));
				var myNewPendingEventsToConsume = new ClientPendingEventsToConsume(rawFlux, new CompletableFuture<>());
				var pendingEventsToConsume = messagesToConsume.computeIfAbsent(channel, n -> myNewPendingEventsToConsume);
				LOG.debug("Received request for channel \"{}\", requesting stream to client", channel);
				if (pendingEventsToConsume instanceof ServerPendingEventsToConsume serverPendingEventsToConsume) {
					//messagesToConsume.remove(channel, pendingEventsToConsume);
					if (!serverPendingEventsToConsume.doneCf.complete(rawFlux)) {
						LOG.error("The server is already producing to channel \"{}\", the request will be rejected", channel);
						return Mono.error(new IllegalStateException("The server is already producing to channel \"" + channel + "\""));
					}
					return Mono.just(DefaultPayload.create("ok", "response"));
				} else if (pendingEventsToConsume == myNewPendingEventsToConsume) {
					//messagesToConsume.remove(channel, pendingEventsToConsume);
					return Mono
							.fromFuture(myNewPendingEventsToConsume::initCf)
							.thenReturn(DefaultPayload.create("ok", "response"));
				} else {
					LOG.warn("Received request for channel \"{}\", but the channel is already active", channel);
					return Mono.error(new IllegalStateException("Channel " + channel + " is already active"));
				}
			} else {
				LOG.warn("Received request for channel \"{}\", but no channel with that name is registered", channel);
				return Mono.error(new IllegalStateException("Channel " + channel + " does not exist, or it has not been registered"));
			}
		} else {
			LOG.warn("Received invalid request");
			return Mono.error(new UnsupportedOperationException("Invalid request"));
		}
	}

	@NotNull
	private <T> Flux<Payload> requestStream(RSocket sendingSocket, Payload payload) {
		if (payload.getMetadataUtf8().equals("notify-can-consume")) {
			var channel = payload.getDataUtf8();
			var producer = producers.get(channel);
			if (producer != null) {
				final ClientPendingEventsToProduce myPendingEventsToProduce = new ClientPendingEventsToProduce(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
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
								.doFinally(s -> {
									messagesToProduce.remove(channel, myPendingEventsToProduce);
									myPendingEventsToProduce.doneCf().complete(null);
								});

					} else {
						LOG.error("The channel \"{}\" is already active", channel);
						return Flux.error(new CancelledChannelException("The channel \"" + channel + "\" is already active"));
					}
				} else {
					LOG.error("The channel \"{}\" is already active", channel);
					return Flux.error(new CancelledChannelException("The channel \"" + channel + "\" is already active"));
				}
			} else {
				LOG.warn("No producer registered for channel \"{}\"", channel);
				return Flux.error(new CancelledChannelException("No producer registered for channel \"" + channel + "\""));
			}
		} else {
			LOG.warn("Received invalid request stream");
			return Flux.error(new CancelledChannelException("Received invalid request stream"));
		}
	}
}
