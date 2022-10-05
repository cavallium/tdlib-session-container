package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.RSocketParameters;
import it.tdlight.reactiveapi.Timestamped;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.logging.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

public class RSocketConsumeAsClient<T> implements EventConsumer<T> {

	private static final Logger LOG = LogManager.getLogger(RSocketConsumeAsClient.class);

	private final HostAndPort host;
	private final ChannelCodec channelCodec;
	private final String channelName;

	public RSocketConsumeAsClient(HostAndPort hostAndPort,
			ChannelCodec channelCodec,
			String channelName) {
		this.channelCodec = channelCodec;
		this.channelName = channelName;
		this.host = hostAndPort;
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
	public Flux<Timestamped<T>> consumeMessages() {
		var deserializer = channelCodec.getNewDeserializer();
		return
				RSocketConnector.create()
						//.resume(new Resume())
						.payloadDecoder(PayloadDecoder.ZERO_COPY)
						.connect(TcpClientTransport.create(host.getHost(), host.getPort()))
						.retryWhen(Retry
								.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
								.maxBackoff(Duration.ofSeconds(16))
								.jitter(1.0)
								.doBeforeRetry(rs -> LOG.warn("Failed to bind, retrying. {}", rs)))
						.flatMapMany(socket -> socket
								.requestStream(DefaultPayload.create("", "consume"))
								.map(payload -> {
									ByteBuf slice = payload.sliceData();
									var data = new byte[slice.readableBytes()];
									slice.readBytes(data, 0, data.length);
									//noinspection unchecked
									return new Timestamped<T>(System.currentTimeMillis(), (T) deserializer.deserialize(null, data));
								})
								.doFinally(signalType -> {
									socket
											.fireAndForget(DefaultPayload.create("", "close"))
											.then(socket.onClose().timeout(Duration.ofSeconds(5), Mono.empty()))
											.doFinally(s -> socket.dispose())
											.onErrorResume(ex -> Mono.empty())
											.subscribeOn(Schedulers.parallel())
											.subscribe();
								})
						)
						.retryWhen(Retry
								.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
								.filter(ex -> ex instanceof ClosedChannelException)
								.maxBackoff(Duration.ofSeconds(16))
								.jitter(1.0)
								.doBeforeRetry(rs -> LOG.warn("Failed to communicate, retrying. {}", rs)))
						.log("RSOCKET_CONSUMER_CLIENT", Level.FINE);
	}
}
