package it.tdlight.tdlibsession.td;

public class TdError extends RuntimeException {

	public TdError(int code, String message) {
		super(code + " " + message);
	}

	public TdError(int code, String message, Throwable cause) {
		super(code + " " + message, cause);
	}
}
