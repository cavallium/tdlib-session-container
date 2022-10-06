package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.SimpleEventProducer;
import it.tdlight.reactiveapi.Timestamped;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Empty;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public class MyRSocketClient implements RSocketChannelManager {

	private final Mono<RSocket> nextClient;
	private final AtomicReference<RSocket> lastClient = new AtomicReference<>();
	private final Empty<Void> disposeRequest = Sinks.empty();

	public MyRSocketClient(HostAndPort baseHost) {
		RetryBackoffSpec retryStrategy = Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(16)).jitter(1.0);
		var transport = TcpClientTransport.create(baseHost.getHost(), baseHost.getPort());

		this.nextClient = RSocketConnector.create()
				//.setupPayload(DefaultPayload.create("client", "setup-info"))
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				//.reconnect(retryStrategy)
				.connect(transport)
				.doOnNext(lastClient::set)
				.cacheInvalidateIf(RSocket::isDisposed);
	}

	@Override
	public <K> EventConsumer<K> registerConsumer(ChannelCodec channelCodec, String channelName) {
		Deserializer<K> deserializer = channelCodec.getNewDeserializer();
		return new EventConsumer<K>() {
			@Override
			public Flux<Timestamped<K>> consumeMessages() {
				return nextClient.flatMapMany(client -> client
						.requestStream(DefaultPayload.create(channelName, "channel"))
						.transform(flux -> RSocketUtils.deserialize(flux, deserializer))
						.map(event -> new Timestamped<>(System.currentTimeMillis(), event)));
			}
		};
	}

	@Override
	public <K> EventProducer<K> registerProducer(ChannelCodec channelCodec, String channelName) {
		Serializer<K> serializer = channelCodec.getNewSerializer();
		return new SimpleEventProducer<K>() {

			@Override
			public Mono<Void> handleSendMessages(Flux<K> eventsFlux) {
				return Mono.defer(() -> {
					Flux<Payload> rawFlux = eventsFlux.transform(flux -> RSocketUtils.serialize(flux, serializer));
					Flux<Payload> combinedRawFlux = Flux.just(DefaultPayload.create(channelName, "channel")).concatWith(rawFlux);
					return nextClient.flatMapMany(client -> client.requestChannel(combinedRawFlux).take(1, true)).then();
				});
			}

		};
	}

	@Override
	public Mono<Void> onClose() {
		return disposeRequest.asMono();
	}

	@Override
	public void dispose() {
		disposeRequest.emitEmpty(EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
		var c = lastClient.get();
		if (c != null) {
			c.dispose();
		}
	}

}
