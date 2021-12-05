package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.AuthenticatedEvent;

/**
 * Any event received from a session
 */
public sealed interface Event permits AuthenticatedEvent {

	/**
	 *
	 * @return temporary unique identifier of the session
	 */
	long sessionId();

	/**
	 * Event received after choosing the user id of the session
	 */
	sealed interface AuthenticatedEvent extends Event permits OnLoginCodeRequested, OnUpdate {

		/**
		 *
		 * @return telegram user id of the session
		 */
		long userId();
	}

	/**
	 * TDLib is asking for an authorization code
	 */
	sealed interface OnLoginCodeRequested extends AuthenticatedEvent
			permits OnBotLoginCodeRequested, OnUserLoginCodeRequested {}

	final record OnUserLoginCodeRequested(long sessionId, long userId, long phoneNumber) implements OnLoginCodeRequested {}

	final record OnBotLoginCodeRequested(long sessionId, long userId, String token) implements OnLoginCodeRequested {}

	/**
	 * Event received from TDLib
	 */
	sealed interface OnUpdate extends AuthenticatedEvent permits OnUpdateData, OnUpdateError {}

	final record OnUpdateData(long sessionId, long userId, TdApi.Update update) implements OnUpdate {}

	final record OnUpdateError(long sessionId, long userId, TdApi.Error error) implements OnUpdate {}
}
