package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.ReactorUtils;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;

public final class RSocketProduceAsServer<K> implements EventProducer<K> {

	private static final Logger LOG = LogManager.getLogger(RSocketProduceAsServer.class);
	private final ChannelCodec channelCodec;
	private final String channelName;
	private final HostAndPort host;

	private final Empty<Void> closeRequest = Sinks.empty();

	public RSocketProduceAsServer(HostAndPort hostAndPort, ChannelCodec channelCodec, String channelName) {
		this.host = hostAndPort;
		this.channelCodec = channelCodec;
		this.channelName = channelName;
	}

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
		return Mono.defer(()-> {
			AtomicReference<CloseableChannel> serverRef = new AtomicReference<>();
			Serializer<K> serializer = channelCodec.getNewSerializer();
			Flux<Payload> serializedEventsFlux = eventsFlux
					.log("RSOCKET_PRODUCER_SERVER", Level.FINE)
					.map(event -> DefaultPayload.create(serializer.serialize(null, event)))
					.doFinally(s -> LOG.debug("Events flux ended: {}", s));

			return RSocketServer
					.create(new SocketAcceptor() {
						@Override
						public @NotNull Mono<RSocket> accept(@NotNull ConnectionSetupPayload setup, @NotNull RSocket sendingSocket) {
							return Mono.just(new RSocket() {
								@Override
								public @NotNull Mono<Void> fireAndForget(@NotNull Payload payload) {
									return Mono.fromRunnable(() -> {
										var srv = serverRef.get();
										if (srv != null) {
											srv.dispose();
										}
									});
								}

								@Override
								public @NotNull Flux<Payload> requestStream(@NotNull Payload payload) {
									return serializedEventsFlux;
								}
							});
						}
					})
					.resume(new Resume())
					.payloadDecoder(PayloadDecoder.ZERO_COPY)
					.bind(TcpServerTransport.create(host.getHost(), host.getPort()))
					.doOnNext(serverRef::set)
					.flatMap(closeableChannel -> closeableChannel.onClose()
							.takeUntilOther(closeRequest.asMono().doFinally(s -> closeableChannel.dispose())));
		});
	}

	@Override
	public void close() {
		closeRequest.tryEmitEmpty();
	}
}
