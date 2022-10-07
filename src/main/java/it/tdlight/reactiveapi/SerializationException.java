package it.tdlight.reactiveapi;

/**
 *  Any exception during serialization in the producer
 */
public class SerializationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SerializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public SerializationException(String message) {
		super(message);
	}

	public SerializationException(Throwable cause) {
		super(cause);
	}

	public SerializationException() {
		super();
	}

	/* avoid the expensive and useless stack trace for serialization exceptions */
	@Override
	public Throwable fillInStackTrace() {
		return this;
	}

}