package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.Ignored;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnResponse.Response;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;

abstract class BaseAtomixReactiveApiClient implements ReactiveApiMultiClient {

	private static final Logger LOG = LogManager.getLogger(BaseAtomixReactiveApiClient.class);
	private static final long EMPTY_USER_ID = 0;

	// Temporary id used to make requests
	private final long clientId;
	private final Consumer<OnRequest<?>> requests;
	private final Map<Long, MonoSink<Timestamped<OnResponse<Object>>>> responses
			= new ConcurrentHashMap<>();
	private final AtomicLong requestId = new AtomicLong(0);
	private final Disposable subscription;

	public BaseAtomixReactiveApiClient(TdlibChannelsSharedReceive sharedTdlibClients) {
		this.clientId = System.nanoTime();
		this.requests = sharedTdlibClients::emitRequest;

		this.subscription = sharedTdlibClients.responses().doOnNext(response -> {
			var responseSink = responses.get(response.data().requestId());
			if (responseSink == null) {
				LOG.debug("Bot received a response for an unknown request id: {}", response.data().requestId());
				return;
			}
			responseSink.success(response);
		}).subscribeOn(Schedulers.parallel())
				.subscribe(v -> {}, ex -> LOG.error("Reactive api client responses flux has failed unexpectedly!", ex));
	}

	@Override
	public <T extends Object> Mono<T> request(long userId, Function<T> request, Instant timeout) {
		return Mono.defer(() -> {
			var requestId = this.requestId.getAndIncrement();
			var timeoutError = new TdError(408, "Request Timeout");
			var requestTimestamp = Instant.now();
			var timeoutDuration = Duration.between(requestTimestamp, timeout);
			if (timeoutDuration.isNegative() || timeoutDuration.isZero()) {
				return Mono.error(timeoutError);
			}
			Mono<T> response = Mono
					.<Timestamped<OnResponse<TdApi.Object>>>create(sink -> {
						sink.onDispose(() -> this.responses.remove(requestId, sink));
						var prev = this.responses.putIfAbsent(requestId, sink);
						if (prev != null) {
							sink.error(new IllegalStateException("Can't call the same request twice: " + requestId));
						}
					})
					.timeout(timeoutDuration, Mono.fromSupplier(() -> new Timestamped<>(requestTimestamp.toEpochMilli(),
							new Response<>(clientId, requestId, userId, new TdApi.Error(408, "Request Timeout")))))
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
					});
			requests.accept(new Request<>(userId, clientId, requestId, request, timeout));
			return response;
		});
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

	static @NotNull ClientBoundEvent deserializeEvent(DataInput is) throws IOException {
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
	public Mono<Void> close() {
		return Mono.fromRunnable(() -> {
			subscription.dispose();
			long now = System.currentTimeMillis();
			responses.forEach((requestId, cf) -> cf.success(new Timestamped<>(now,
					new Response<>(clientId, requestId, EMPTY_USER_ID, new Error(408, "Request Timeout"))
			)));
			responses.clear();
		});
	}
}
