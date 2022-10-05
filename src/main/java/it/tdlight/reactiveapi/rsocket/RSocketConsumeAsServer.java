package it.tdlight.reactiveapi.rsocket;

import com.google.common.net.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.RSocketParameters;
import it.tdlight.reactiveapi.Timestamped;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class RSocketConsumeAsServer<T> implements EventConsumer<T> {

	private static final Logger LOG = LogManager.getLogger(RSocketConsumeAsServer.class);

	private final HostAndPort host;
	private final ChannelCodec channelCodec;
	private final String channelName;

	public RSocketConsumeAsServer(HostAndPort hostAndPort,
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
		Deserializer<T> deserializer = channelCodec.getNewDeserializer();
		return Mono
				.<Tuple3<CloseableChannel, RSocket, Flux<Timestamped<T>>>>create(sink -> {
					AtomicReference<CloseableChannel> serverRef = new AtomicReference<>();
					var server = RSocketServer
							.create((setup, in) -> {
								var inRawFlux = in.requestStream(DefaultPayload.create("", "consume"));
								var inFlux = inRawFlux.map(payload -> {
									ByteBuf slice = payload.sliceData();
									var data = new byte[slice.readableBytes()];
									slice.readBytes(data, 0, data.length);
									return new Timestamped<>(System.currentTimeMillis(), deserializer.deserialize(null, data));
								});
								sink.success(Tuples.of(serverRef.get(), in, inFlux));

								return Mono.just(new RSocket() {});
							})
							.payloadDecoder(PayloadDecoder.ZERO_COPY)
							.resume(new Resume())
							.bindNow(TcpServerTransport.create(host.getHost(), host.getPort()));
					serverRef.set(server);
					sink.onCancel(server);
				})
				.subscribeOn(Schedulers.boundedElastic())
				.flatMapMany(t -> t.getT3().doFinally(s -> {
					t.getT2().dispose();
					t.getT1().dispose();
				}))
				.log("RSOCKET_CONSUMER_SERVER", Level.FINE);
		}
		/*return Flux.defer(() -> {
			var deserializer = channelCodec.getNewDeserializer();
			AtomicReference<RSocket> inRef = new AtomicReference<>();
			AtomicReference<Subscription> inSubRef = new AtomicReference<>();
			return Flux.<Timestamped<T>>create(sink -> {
						var server = RSocketServer.create((setup, in) -> {
							var prev = inRef.getAndSet(in);
							if (prev != null) {
								prev.dispose();
							}

							var inRawFlux = in.requestStream(DefaultPayload.create("", "consume"));
							var inFlux = inRawFlux.map(payload -> {
								ByteBuf slice = payload.sliceData();
								var data = new byte[slice.readableBytes()];
								slice.readBytes(data, 0, data.length);
								//noinspection unchecked
								return new Timestamped<>(System.currentTimeMillis(), (T) deserializer.deserialize(null, data));
							});

							inFlux.subscribe(new CoreSubscriber<>() {
								@Override
								public void onSubscribe(@NotNull Subscription s) {
									var prevS = inSubRef.getAndSet(s);
									if (prevS != null) {
										prevS.cancel();
									} else {
										sink.onRequest(n -> {
											s.request(n);
										});
									}
								}

								@Override
								public void onNext(Timestamped<T> val) {
									sink.next(val);
								}

								@Override
								public void onError(Throwable throwable) {
									sink.error(throwable);
								}

								@Override
								public void onComplete() {
									sink.complete();
								}
							});

							return Mono.just(new RSocket() {});
						}).payloadDecoder(PayloadDecoder.ZERO_COPY).bindNow(TcpServerTransport.create(host.getHost(), host.getPort()));
						sink.onCancel(() -> {
							var inSub = inSubRef.get();
							if (inSub != null) {
								inSub.cancel();
							}
						});
						sink.onDispose(() -> {
							var in = inRef.get();
							if (in != null) {
								in.dispose();
							}
							server.dispose();
						});
					}).subscribeOn(Schedulers.boundedElastic()).log("RSOCKET_CONSUMER_SERVER", Level.FINE)
					.retryWhen(Retry
							.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
							.maxBackoff(Duration.ofSeconds(16))
							.jitter(1.0)
							.doBeforeRetry(rs -> LOG.warn("Failed to consume as server, retrying. {}", rs)));
		});*/
		/*
		return Flux.<Timestamped<T>>create(sink -> {
					RSocketServer
							.create((setup, socket) -> {
								socket.requestStream(DefaultPayload.create("", "consume")).map(payload -> {
									ByteBuf slice = payload.sliceData();
									var data = new byte[slice.readableBytes()];
									slice.readBytes(data, 0, data.length);
									//noinspection unchecked
									return new Timestamped<>(System.currentTimeMillis(), (T) deserializer.deserialize(null, data));
								}).subscribe(new CoreSubscriber<>() {
									@Override
									public void onSubscribe(@NotNull Subscription s) {
										sink.onDispose(() -> {
											s.cancel();
											socket.dispose();
										});
										sink.onRequest(n -> {
											if (n > 8192) {
												throw new UnsupportedOperationException(
														"Requests count is bigger than max buffer size! " + n + " > " + 8192);
											}
											s.request(n);
										});
										sink.onCancel(() -> s.cancel());
									}

									@Override
									public void onNext(Timestamped<T> val) {
										sink.next(val);
									}

									@Override
									public void onError(Throwable throwable) {
										sink.error(throwable);
									}

									@Override
									public void onComplete() {
										sink.complete();
									}
								});
								return Mono.just(socket);
							})
							.payloadDecoder(PayloadDecoder.ZERO_COPY)
							.bind(TcpServerTransport.create(host.getHost(), host.getPort()))
							.subscribeOn(Schedulers.parallel())
							.subscribe(v -> {
								sink.onDispose(v);
							}, sink::error, sink::complete);
				})
				.retryWhen(Retry
						.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
						.maxBackoff(Duration.ofSeconds(16))
						.jitter(1.0)
						.doBeforeRetry(rs -> LOG.warn("Failed to consume as server, retrying. {}", rs)));
						*/
}
