package it.tdlight.reactiveapi.rsocket;

import java.nio.channels.ClosedChannelException;

public class CancelledChannelException extends java.io.IOException {

	public CancelledChannelException() {
	}

	public CancelledChannelException(String message) {
		super(message);
	}

	public CancelledChannelException(String message, Throwable cause) {
		super(message, cause);
	}

	public CancelledChannelException(Throwable cause) {
		super(cause);
	}

}
