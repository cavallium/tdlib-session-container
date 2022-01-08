package it.tdlight.reactiveapi;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.Subscription;
import io.atomix.core.Atomix;
import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import it.tdlight.reactiveapi.Event.Request;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.SerializationException;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;

public class LiveAtomixReactiveApiClient implements ReactiveApiClient {

	private final ClusterEventService eventService;
	private final long liveId;
	private final long userId;

	private final Flux<ClientBoundEvent> clientBoundEvents;

	public LiveAtomixReactiveApiClient(Atomix atomix, long liveId, long userId) {
		this.eventService = atomix.getEventService();
		this.liveId = liveId;
		this.userId = userId;
		this.clientBoundEvents = Flux
				.<ClientBoundEvent>push(sink -> {
					var subscriptionFuture = eventService.subscribe("session-client-bound-events", LiveAtomixReactiveApiClient::deserializeEvent, s -> {
						sink.next(s);
						return CompletableFuture.completedFuture(null);
					}, (a) -> null);
					sink.onDispose(() -> subscriptionFuture.thenAccept(Subscription::close));
				}, OverflowStrategy.ERROR)
				.filter(e -> e.userId() == userId && e.liveId() == liveId)
				.onBackpressureBuffer(0xFFFF, BufferOverflowStrategy.ERROR)
				.share();
	}

	@Override
	public Flux<ClientBoundEvent> clientBoundEvents() {
		return clientBoundEvents;
	}
	
	@Override
	public <T extends TdApi.Object> Mono<T> request(TdApi.Function<T> request, Instant timeout) {
		return Mono.fromCompletionStage(() -> eventService.send("session-" + liveId + "-requests",
				new Request<>(liveId, request, timeout),
				LiveAtomixReactiveApiClient::serializeRequest,
				LiveAtomixReactiveApiClient::deserializeResponse,
				Duration.between(Instant.now(), timeout)
		)).handle((item, sink) -> {
			if (item instanceof TdApi.Error error) {
				sink.error(new TdError(error.code, error.message));
			} else {
				//noinspection unchecked
				sink.next((T) item);
			}
		});
	}

	@Override
	public long getUserId() {
		return userId;
	}

	static TdApi.Object deserializeResponse(byte[] bytes) {
		try {
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
			try (var dataInputStream = new DataInputStream(byteArrayInputStream)) {
				var liveId = dataInputStream.readLong();
				var userId = dataInputStream.readLong();
				return switch (dataInputStream.readByte()) {
					case 0x01 -> new OnUpdateData(liveId, userId, (TdApi.Update) TdApi.Deserializer.deserialize(dataInputStream));
					case 0x02 -> new OnUpdateError(liveId, userId, (TdApi.Error) TdApi.Deserializer.deserialize(dataInputStream));
					case 0x03 -> new OnUserLoginCodeRequested(liveId, userId, dataInputStream.readLong());
					case 0x04 -> new OnBotLoginCodeRequested(liveId, userId, dataInputStream.readUTF());
					case 0x05 -> new OnOtherDeviceLoginRequested(liveId, userId, dataInputStream.readUTF());
					default -> throw new IllegalStateException("Unexpected value: " + dataInputStream.readByte());
				};
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}
}
