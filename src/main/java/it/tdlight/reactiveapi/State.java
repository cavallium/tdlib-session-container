package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.AuthPhase.AUTH_PHASE;
import static it.tdlight.reactiveapi.AuthPhase.BROKEN;
import static it.tdlight.reactiveapi.AuthPhase.ENCRYPTION_PHASE;
import static it.tdlight.reactiveapi.AuthPhase.LOGGED_IN;
import static it.tdlight.reactiveapi.AuthPhase.LOGGED_OUT;
import static it.tdlight.reactiveapi.AuthPhase.LOGGING_OUT;
import static it.tdlight.reactiveapi.AuthPhase.PARAMETERS_PHASE;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.tdlight.common.Signal;
import it.tdlight.jni.TdApi;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RecordBuilder
public record State(AuthPhase authPhase) implements StateBuilder.With {

	private static final Logger LOG = LoggerFactory.getLogger(State.class);

	public State withSignal(Signal signal) {
		var newState = this;

		// Mark state as broken if the connection is errored unexpectedly
		if (signal.isException()) {
			newState = newState.withAuthPhase(BROKEN);
		}

		newState = switch (newState.authPhase) {
			// Mark state as broken if the connection is terminated unexpectedly
			case PARAMETERS_PHASE, ENCRYPTION_PHASE, AUTH_PHASE, LOGGED_IN -> {
				if (signal.isClosed()) {
					yield newState.withAuthPhase(BROKEN);
				} else {
					yield newState;
				}
			}
			case LOGGING_OUT -> {
				// Mark state as logged out if the connection is terminated successfully
				if (signal.isClosed()) {
					yield newState.withAuthPhase(LOGGED_OUT);
				} else {
					yield newState;
				}
			}
			default -> newState;
		};

		if (newState.authPhase != BROKEN && signal.isUpdate()) {
			var update = signal.getUpdate();
			newState = switch (update.getConstructor()) {
				// Forcefully logout if the update stream fails
				case TdApi.Error.CONSTRUCTOR -> newState.withAuthPhase(LOGGED_OUT);
				case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
					var updateAuthState = (TdApi.UpdateAuthorizationState) update;
					yield switch (updateAuthState.authorizationState.getConstructor()) {
						case TdApi.AuthorizationStateClosing.CONSTRUCTOR -> {
							if (newState.authPhase != LOGGED_IN) {
								LOG.warn("Logging out, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(LOGGED_IN)
								);
							}
							yield newState.withAuthPhase(LOGGING_OUT);
						}
						case TdApi.AuthorizationStateClosed.CONSTRUCTOR -> {
							if (newState.authPhase != LOGGING_OUT) {
								LOG.warn("Logged out, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(LOGGING_OUT)
								);
							}
							yield newState.withAuthPhase(LOGGED_OUT);
						}
						case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR -> {
							if (newState.authPhase != LOGGED_OUT) {
								LOG.warn("Waiting parameters, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(LOGGED_OUT)
								);
							}
							yield newState.withAuthPhase(PARAMETERS_PHASE);
						}
						case TdApi.AuthorizationStateWaitEncryptionKey.CONSTRUCTOR -> {
							if (newState.authPhase != PARAMETERS_PHASE) {
								LOG.warn("Waiting parameters, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(PARAMETERS_PHASE)
								);
							}
							yield newState.withAuthPhase(ENCRYPTION_PHASE);
						}
						case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR,
								TdApi.AuthorizationStateWaitRegistration.CONSTRUCTOR,
								TdApi.AuthorizationStateWaitCode.CONSTRUCTOR,
								TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR,
								TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR -> {
							if (newState.authPhase != ENCRYPTION_PHASE && newState.authPhase != AUTH_PHASE) {
								LOG.warn(
										"Waiting for authentication, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(ENCRYPTION_PHASE, AUTH_PHASE)
								);
							}
							yield newState.withAuthPhase(AUTH_PHASE);
						}
						case TdApi.AuthorizationStateReady.CONSTRUCTOR -> {
							if (newState.authPhase != ENCRYPTION_PHASE && newState.authPhase != AUTH_PHASE) {
								LOG.warn("Logged in, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(ENCRYPTION_PHASE, AUTH_PHASE)
								);
							}
							yield newState.withAuthPhase(LOGGED_IN);
						}
						case TdApi.AuthorizationStateLoggingOut.CONSTRUCTOR -> {
							if (newState.authPhase != LOGGED_IN) {
								LOG.warn("Logged in, but the current auth phase is {} instead of {}",
										newState.authPhase,
										Set.of(LOGGED_IN)
								);
							}
							yield newState.withAuthPhase(LOGGING_OUT);
						}
						default -> {
							LOG.error("Unknown authorization state: {}", updateAuthState.authorizationState);
							yield newState;
						}
					};
				}
				default -> newState;
			};
		}
		return newState;
	}
}
