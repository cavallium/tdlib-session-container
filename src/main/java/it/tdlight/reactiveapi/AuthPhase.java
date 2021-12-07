package it.tdlight.reactiveapi;

public enum AuthPhase {
	LOGGED_OUT,
	PARAMETERS_PHASE,
	ENCRYPTION_PHASE,
	AUTH_PHASE,
	LOGGED_IN,
	LOGGING_OUT,
	/**
	 * Similar to {@link #LOGGED_OUT}, but it can't be recovered
 	 */
	BROKEN
}
