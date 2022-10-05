package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.RSocketParameters;
import it.tdlight.reactiveapi.ReactorUtils;
import it.tdlight.reactiveapi.Timestamped;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.logging.Level;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.util.retry.Retry;

public final class RSocketProduceAsClient<K> implements EventProducer<K> {

	private static final Logger LOG = LogManager.getLogger(RSocketProduceAsClient.class);
	private final ChannelCodec channelCodec;
	private final String channelName;
	private final HostAndPort host;
	private final Empty<Void> closeRequest = Sinks.empty();

	public RSocketProduceAsClient(HostAndPort host, ChannelCodec channelCodec, String channelName) {
		this.channelCodec = channelCodec;
		this.channelName = channelName;
		this.host = host;
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
		Serializer<K> serializer = channelCodec.getNewSerializer();
		Flux<Payload> serializedEventsFlux = eventsFlux
				.map(event -> DefaultPayload.create(serializer.serialize(null, event)))
				.log("RSOCKET_PRODUCER_CLIENT", Level.FINE)
				.doFinally(s -> LOG.debug("Events flux ended: {}", s));

		return
				RSocketConnector.create()
						.payloadDecoder(PayloadDecoder.ZERO_COPY)
						.setupPayload(DefaultPayload.create("", "connect"))
						.acceptor(SocketAcceptor.forRequestStream(payload  -> serializedEventsFlux))
						//.resume(new Resume())
						.connect(TcpClientTransport.create(host.getHost(), host.getPort()))
						.retryWhen(Retry
								.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
								.maxBackoff(Duration.ofSeconds(16))
								.jitter(1.0)
								.doBeforeRetry(rs -> LOG.warn("Failed to bind, retrying. {}", rs)))
						.flatMap(rSocket -> rSocket.onClose()
								.takeUntilOther(closeRequest.asMono().doFinally(s -> rSocket.dispose())))
						.retryWhen(Retry
								.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
								.filter(ex -> ex instanceof ClosedChannelException)
								.maxBackoff(Duration.ofSeconds(16))
								.jitter(1.0)
								.doBeforeRetry(rs -> LOG.warn("Failed to communicate, retrying. {}", rs)))
						.log("RSOCKET_PRODUCER_CLIENT_Y", Level.FINE);
	}

	@Override
	public void close() {
		closeRequest.tryEmitEmpty();
	}
}
