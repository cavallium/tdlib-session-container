package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;

public class TdError extends Exception {

	private final int code;
	private final String message;

	public TdError(int code, String message) {
		super(code + " " + message);
		this.code = code;
		this.message = message;
	}

	public TdError(int code, String message, Throwable cause) {
		super(code + " " + message, cause);
		this.code = code;
		this.message = message;
	}

	public int getTdCode() {
		return code;
	}

	public String getTdMessage() {
		return message;
	}

	public TdApi.Error getTdError() {
		return new Error(code, message);
	}
}
