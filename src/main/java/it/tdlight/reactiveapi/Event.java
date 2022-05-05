package it.tdlight.reactiveapi;

import it.tdlight.common.utils.LibraryVersion;
import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest.InvalidRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import it.tdlight.reactiveapi.Event.ServerBoundEvent;
import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationException;
import org.jetbrains.annotations.Nullable;

/**
 * Any event received from a session
 */
public sealed interface Event {

	int SERIAL_VERSION = ArrayUtils.hashCode(LibraryVersion.VERSION.getBytes(StandardCharsets.US_ASCII));

	/**
	 *
	 * @return temporary unique identifier of the session
	 */
	long liveId();

	/**
	 * Event received after choosing the user id of the session
	 */
	sealed interface ClientBoundEvent extends Event {

		/**
		 *
		 * @return telegram user id of the session
		 */
		long userId();
	}

	sealed interface ServerBoundEvent extends Event {}

	/**
	 * TDLib is asking for an authorization code
	 */
	sealed interface OnLoginCodeRequested extends ClientBoundEvent {}

	record OnUserLoginCodeRequested(long liveId, long userId, long phoneNumber) implements OnLoginCodeRequested {}

	record OnBotLoginCodeRequested(long liveId, long userId, String token) implements OnLoginCodeRequested {}

	record OnOtherDeviceLoginRequested(long liveId, long userId, String link) implements ClientBoundEvent {}

	record OnPasswordRequested(long liveId, long userId, String passwordHint, boolean hasRecoveryEmail,
														 String recoveryEmailPattern) implements ClientBoundEvent {}

	record Ignored(long liveId, long userId) implements ClientBoundEvent {}

	/**
	 * Event received from TDLib
	 */
	sealed interface OnUpdate extends ClientBoundEvent {}

	record OnUpdateData(long liveId, long userId, TdApi.Update update) implements OnUpdate {}

	record OnUpdateError(long liveId, long userId, TdApi.Error error) implements OnUpdate {}

	sealed interface OnRequest<T extends TdApi.Object> extends ServerBoundEvent {

		record Request<T extends TdApi.Object>(long liveId, TdApi.Function<T> request, Instant timeout)
				implements OnRequest<T> {}

		record InvalidRequest<T extends TdApi.Object>(long liveId) implements OnRequest<T> {}

		static <T extends TdApi.Object> Event.OnRequest<T> deserialize(DataInput dataInput) {
			try {
				var liveId = dataInput.readLong();
				var dataVersion = dataInput.readInt();
				if (dataVersion != SERIAL_VERSION) {
					// Deprecated request
					return new InvalidRequest<>(liveId);
				}
				long millis = dataInput.readLong();
				var timeout = Instant.ofEpochMilli(millis);
				@SuppressWarnings("unchecked")
				TdApi.Function<T> request = (TdApi.Function<T>) TdApi.Deserializer.deserialize(dataInput);
				return new Request<>(liveId, request, timeout);
			} catch (UnsupportedOperationException | IOException e) {
				throw new SerializationException(e);
			}
		}
	}
}
