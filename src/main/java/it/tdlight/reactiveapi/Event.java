package it.tdlight.reactiveapi;

import it.tdlight.common.utils.LibraryVersion;
import it.tdlight.jni.TdApi;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

/**
 * Any event received from a session
 */
public sealed interface Event {

	int SERIAL_VERSION = Arrays.hashCode(LibraryVersion.VERSION.getBytes(StandardCharsets.US_ASCII));

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

	record OnUserLoginCodeRequested(long userId, long phoneNumber) implements OnLoginCodeRequested {}

	record OnBotLoginCodeRequested(long userId, String token) implements OnLoginCodeRequested {}

	record OnOtherDeviceLoginRequested(long userId, String link) implements ClientBoundEvent {}

	record OnPasswordRequested(long userId, String passwordHint, boolean hasRecoveryEmail,
														 String recoveryEmailPattern) implements ClientBoundEvent {}

	record Ignored(long userId) implements ClientBoundEvent {}

	/**
	 * Event received from TDLib
	 */
	sealed interface OnUpdate extends ClientBoundEvent {}

	record OnUpdateData(long userId, TdApi.Update update) implements OnUpdate {}

	record OnUpdateError(long userId, TdApi.Error error) implements OnUpdate {}

	sealed interface OnRequest<T extends TdApi.Object> extends ServerBoundEvent {

		record Request<T extends TdApi.Object>(long userId, long clientId, long requestId, TdApi.Function<T> request,
																					 Instant timeout) implements OnRequest<T> {}

		record InvalidRequest<T extends TdApi.Object>(long userId, long clientId, long requestId) implements OnRequest<T> {}

		long userId();

		long clientId();

		long requestId();

	}

	sealed interface OnResponse<T extends TdApi.Object> extends ClientBoundEvent {

		record Response<T extends TdApi.Object>(long clientId, long requestId, long userId,
																						T response) implements OnResponse<T> {}

		record InvalidResponse<T extends TdApi.Object>(long clientId, long requestId, long userId) implements
				OnResponse<T> {}

		long clientId();

		long requestId();
	}
}
