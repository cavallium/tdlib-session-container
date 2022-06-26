package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.common.utils.LibraryVersion;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.Ignored;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.InvalidResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.errors.SerializationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

abstract class BaseAtomixReactiveApiClient implements ReactiveApiClient, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(BaseAtomixReactiveApiClient.class);

	private static final Duration TEN_MS = Duration.ofMillis(10);

	protected final long userId;
	// Temporary id used to make requests
	private final long clientId;
	private final Many<OnRequest<?>> requests
			= Sinks.many().unicast().onBackpressureBuffer(Queues.<Event.OnRequest<?>>small().get());
	private final Map<Long, CompletableFuture<Timestamped<OnResponse<TdApi.Object>>>> responses
			= new ConcurrentHashMap<>();
	private final AtomicLong requestId = new AtomicLong(0);
	private final Disposable subscription;

	public BaseAtomixReactiveApiClient(KafkaTdlibClient kafkaTdlibClient, long userId) {
		this.userId = userId;
		this.clientId = System.nanoTime();
		var subscription1 = kafkaTdlibClient.request().sendMessages(userId, requests.asFlux())
				.subscribeOn(Schedulers.boundedElastic())
				.subscribe(v -> {}, ex -> LOG.error("Failed to send requests", ex));

		var subscription2 = kafkaTdlibClient.response()
				.consumeMessages("td-responses", userId)
				.filter(response -> response.data().clientId() == clientId)
				.doOnNext(response -> {
					var responseSink = responses.get(response.data().requestId());
					if (responseSink == null) {
						LOG.debug("Bot #IDU{} received a response for an unknown request id: {}",
								userId, response.data().requestId());
						return;
					}
					responseSink.complete(response);
				})
				.subscribeOn(Schedulers.parallel())
				.subscribe();
		this.subscription = () -> {
			subscription1.dispose();
			subscription2.dispose();
		};
	}

	@Override
	public final <T extends TdApi.Object> Mono<T> request(TdApi.Function<T> request, Instant timeout) {
		return Mono.defer(() -> {
			var requestId = this.requestId.getAndIncrement();
			var timeoutError = new TdError(408, "Request Timeout");
			Mono<T> timeoutErrorMono = Mono.error(timeoutError);
			var timeoutDuration = Duration.between(Instant.now(), timeout);
			if (timeoutDuration.isNegative() || timeoutDuration.isZero()) {
				return timeoutErrorMono;
			}
			var cf = new CompletableFuture<Timestamped<OnResponse<TdApi.Object>>>();
			this.responses.put(requestId, cf);
			Mono<T> response = Mono.fromFuture(cf)
					.<T>handle((responseObj, sink) -> {
						if (Instant.ofEpochMilli(responseObj.timestamp()).compareTo(timeout) > 0) {
							sink.error(new TdError(408, "Request Timeout"));
						} else if (responseObj.data() instanceof OnResponse.InvalidResponse<?>) {
							sink.error(new TdError(400, "Conflicting protocol version"));
						} else if (responseObj.data() instanceof OnResponse.Response<?> onResponse) {
							if (onResponse.response().getConstructor() == Error.CONSTRUCTOR) {
								var tdError = (TdApi.Error) onResponse.response();
								sink.error(new TdError(tdError.code, tdError.message));
							} else {
								//noinspection unchecked
								var tdResponse = (T) onResponse.response();
								sink.next(tdResponse);
							}
						} else {
							sink.error(new UnsupportedOperationException("Unknown response type: " + responseObj.data().getClass()));
						}
					})
					.timeout(timeoutDuration, timeoutErrorMono)
					.doFinally(s -> this.responses.remove(requestId));
			requests.emitNext(new Request<>(clientId, requestId, request, timeout), EmitFailureHandler.busyLooping(TEN_MS));
			return response;
		});
	}

	@Override
	public final long getUserId() {
		return userId;
	}

	@Override
	public final boolean isPullMode() {
		return true;
	}


	static ClientBoundEvent deserializeEvent(byte[] bytes) {
		try (var byteArrayInputStream = new ByteArrayInputStream(bytes)) {
			try (var is = new DataInputStream(byteArrayInputStream)) {
				return deserializeEvent(is);
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	static @NotNull ClientBoundEvent deserializeEvent(DataInputStream is) throws IOException {
		var userId = is.readLong();
		var dataVersion = is.readInt();
		if (dataVersion != SERIAL_VERSION) {
			return new Ignored(userId);
		}
		return switch (is.readByte()) {
			case 0x01 -> new OnUpdateData(userId, (TdApi.Update) TdApi.Deserializer.deserialize(is));
			case 0x02 -> new OnUpdateError(userId, (TdApi.Error) TdApi.Deserializer.deserialize(is));
			case 0x03 -> new OnUserLoginCodeRequested(userId, is.readLong());
			case 0x04 -> new OnBotLoginCodeRequested(userId, is.readUTF());
			case 0x05 -> new OnOtherDeviceLoginRequested(userId, is.readUTF());
			case 0x06 -> new OnPasswordRequested(userId, is.readUTF(), is.readBoolean(), is.readUTF());
			default -> throw new IllegalStateException("Unexpected value: " + is.readByte());
		};
	}

	@Override
	public void close() {
		subscription.dispose();
		long now = System.currentTimeMillis();
		responses.forEach((requestId, cf) -> cf.complete(new Timestamped<>(now,
				new Response<>(clientId, requestId, userId, new Error(408, "Request Timeout"))
		)));
		responses.clear();
	}
}
