package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.ServerBoundEvent;
import java.io.DataInput;
import java.io.IOException;
import java.time.Instant;
import org.apache.commons.lang3.SerializationException;

/**
 * Any event received from a session
 */
public sealed interface Event permits ClientBoundEvent, ServerBoundEvent {

	/**
	 *
	 * @return temporary unique identifier of the session
	 */
	long liveId();

	/**
	 * Event received after choosing the user id of the session
	 */
	sealed interface ClientBoundEvent extends Event permits OnLoginCodeRequested, OnOtherDeviceLoginRequested,
			OnPasswordRequested, OnUpdate {

		/**
		 *
		 * @return telegram user id of the session
		 */
		long userId();
	}

	sealed interface ServerBoundEvent extends Event permits Request {}

	/**
	 * TDLib is asking for an authorization code
	 */
	sealed interface OnLoginCodeRequested extends ClientBoundEvent
			permits OnBotLoginCodeRequested, OnUserLoginCodeRequested {}

	record OnUserLoginCodeRequested(long liveId, long userId, long phoneNumber) implements OnLoginCodeRequested {}

	record OnBotLoginCodeRequested(long liveId, long userId, String token) implements OnLoginCodeRequested {}

	record OnOtherDeviceLoginRequested(long liveId, long userId, String link) implements ClientBoundEvent {}

	record OnPasswordRequested(long liveId, long userId, String passwordHint, boolean hasRecoveryEmail,
														 String recoveryEmailPattern) implements ClientBoundEvent {}

	/**
	 * Event received from TDLib
	 */
	sealed interface OnUpdate extends ClientBoundEvent permits OnUpdateData, OnUpdateError {}

	record OnUpdateData(long liveId, long userId, TdApi.Update update) implements OnUpdate {}

	record OnUpdateError(long liveId, long userId, TdApi.Error error) implements OnUpdate {}

	record Request<T extends TdApi.Object>(long liveId, TdApi.Function<T> request, Instant timeout) implements
			ServerBoundEvent {

		public static <T extends TdApi.Object> Request<T> deserialize(DataInput dataInput) {
			try {
				var liveId = dataInput.readLong();
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
