package it.tdlight.reactiveapi;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.core.Atomix;
import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import it.tdlight.reactiveapi.Event.Request;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

abstract class BaseAtomixReactiveApiClient implements ReactiveApiClient, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(BaseAtomixReactiveApiClient.class);

	protected final ClusterEventService eventService;
	protected final long userId;
	private Disposable liveIdChangeSubscription;
	private Flux<Long> liveIdChange;
	private Mono<Long> emptyIdErrorMono;

	public BaseAtomixReactiveApiClient(Atomix atomix, long userId) {
		this.eventService = atomix.getEventService();
		this.userId = userId;
	}

	protected void initialize() {
		this.liveIdChange = liveIdChange().cache(1);
		this.liveIdChangeSubscription = liveIdChange
				.subscribeOn(Schedulers.parallel())
				.subscribe(v -> LOG.debug("Live id of user {} changed: {}", userId, v),
						ex -> LOG.error("Failed to retrieve live id of user {}", userId)
				);
		this.emptyIdErrorMono = Mono.error(() -> new TdError(404, "Bot #IDU" + this.userId
				+ " is not found on the cluster, no live id has been associated with it locally"));
	}

	@Override
	public final <T extends TdApi.Object> Mono<T> request(TdApi.Function<T> request, Instant timeout) {
		return liveIdChange
				.take(1, true)
				.singleOrEmpty()
				.switchIfEmpty(emptyIdErrorMono)
				.flatMap(liveId -> AtomixUtils
						.fromCf(() -> eventService.send("session-" + liveId + "-requests",
								new Request<>(liveId, request, timeout),
								LiveAtomixReactiveApiClient::serializeRequest,
								LiveAtomixReactiveApiClient::deserializeResponse,
								Duration.between(Instant.now(), timeout)
						))
						.subscribeOn(Schedulers.boundedElastic())
						.onErrorMap(ex -> {
							if (ex instanceof MessagingException.NoRemoteHandler) {
								return new TdError(404, "Bot #IDU" + this.userId + " (liveId: " + liveId + ") is not found on the cluster");
							} else if (ex instanceof TimeoutException || ex instanceof ConnectException) {
								return new TdError(408, "Request Timeout", ex);
							} else {
								return ex;
							}
						})
				)
				.<T>handle((item, sink) -> {
					if (item instanceof TdApi.Error error) {
						sink.error(new TdError(error.code, error.message));
					} else {
						//noinspection unchecked
						sink.next((T) item);
					}
				})
				.onErrorMap(ex -> {
					if (ex instanceof MessagingException.NoRemoteHandler) {
						return new TdError(404, "Bot #IDU" + this.userId + " is not found on the cluster");
					} else if (ex instanceof TimeoutException || ex instanceof ConnectException) {
						return new TdError(408, "Request Timeout", ex);
					} else {
						return ex;
					}
				});
	}

	protected abstract Flux<Long> liveIdChange();

	@Override
	public final long getUserId() {
		return userId;
	}

	@Override
	public final boolean isPullMode() {
		return true;
	}


	static TdApi.Object deserializeResponse(byte[] bytes) {
		try {
			if (bytes == null || bytes.length == 0) {
				return null;
			}
			return TdApi.Deserializer.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	static byte[] serializeRequest(Request<?> request) {
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				dataOutputStream.writeLong(request.liveId());
				dataOutputStream.writeLong(request.timeout().toEpochMilli());
				request.request().serialize(dataOutputStream);
				dataOutputStream.flush();
				return byteArrayOutputStream.toByteArray();
			}
		} catch (UnsupportedOperationException | IOException ex) {
			throw new SerializationException(ex);
		}
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

	static List<ClientBoundEvent> deserializeEvents(byte[] bytes) {
		try (var byteArrayInputStream = new ByteArrayInputStream(bytes)) {
			try (var is = new DataInputStream(byteArrayInputStream)) {
				var len = is.readInt();
				var result = new ArrayList<ClientBoundEvent>(len);
				for (int i = 0; i < len; i++) {
					result.add(deserializeEvent(is));
				}
				return result;
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	static ClientBoundEvent deserializeEvent(DataInputStream is) throws IOException {
		var liveId = is.readLong();
		var userId = is.readLong();
		return switch (is.readByte()) {
			case 0x01 -> new OnUpdateData(liveId, userId, (TdApi.Update) TdApi.Deserializer.deserialize(is));
			case 0x02 -> new OnUpdateError(liveId, userId, (TdApi.Error) TdApi.Deserializer.deserialize(is));
			case 0x03 -> new OnUserLoginCodeRequested(liveId, userId, is.readLong());
			case 0x04 -> new OnBotLoginCodeRequested(liveId, userId, is.readUTF());
			case 0x05 -> new OnOtherDeviceLoginRequested(liveId, userId, is.readUTF());
			case 0x06 -> new OnPasswordRequested(liveId, userId, is.readUTF(), is.readBoolean(), is.readUTF());
			default -> throw new IllegalStateException("Unexpected value: " + is.readByte());
		};
	}

	@Override
	public void close() {
		if (liveIdChangeSubscription != null) {
			liveIdChangeSubscription.dispose();
		}
	}
}
