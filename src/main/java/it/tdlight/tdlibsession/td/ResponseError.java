package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ResponseError extends IOException {

	@NotNull
	private final String botName;
	@NotNull
	private final String tag;
	private final int code;
	@NotNull
	private final String message;
	private final Throwable cause;

	public ResponseError(@NotNull Function function, @NotNull String botName, @NotNull TdApi.Error cause) {
		super("Bot '" + botName + "' failed the request '" + functionToInlineString(function) + "': " + cause.code + " " + cause.message);
		this.botName = botName;
		this.tag = functionToInlineString(function);
		this.code = cause.code;
		this.message = cause.message;
		this.cause = null;
	}

	public ResponseError(@NotNull String tag, @NotNull String botName, @NotNull TdApi.Error cause) {
		super("Bot '" + botName + "' failed the request '" + tag + "': " + cause.code + " " + cause.message);
		this.botName = botName;
		this.tag = tag;
		this.code = cause.code;
		this.message = cause.message;
		this.cause = null;
	}

	public ResponseError(@NotNull Function function, @NotNull String botName, @NotNull Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + functionToInlineString(function) + "': " + cause.getMessage());
		this.botName = botName;
		this.tag = functionToInlineString(function);
		this.code = 500;
		this.message = cause.getMessage();
		this.cause = cause;
	}

	public ResponseError(@NotNull String tag, @NotNull String botName, @NotNull Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + tag + "': " + cause.getMessage());
		this.botName = botName;
		this.tag = tag;
		this.code = 500;
		this.message = cause.getMessage();
		this.cause = cause;
	}

	public static ResponseError newResponseError(@NotNull Function function, @NotNull String botName, @NotNull TdApi.Error cause) {
		return new ResponseError(function, botName, cause);
	}

	public static ResponseError newResponseError(@NotNull String tag, @NotNull String botName, @NotNull TdApi.Error cause) {
		return new ResponseError(tag, botName, cause);
	}

	public static ResponseError newResponseError(@NotNull Function function, @NotNull String botName, @NotNull Throwable cause) {
		return new ResponseError(function, botName, cause);
	}

	public static ResponseError newResponseError(@NotNull String tag, @NotNull String botName, @NotNull Throwable cause) {
		return new ResponseError(tag, botName, cause);
	}

	@Nullable
	public static <T> T get(@NotNull Function function, @NotNull String botName, CompletableFuture<T> action) throws ResponseError {
		try {
			return action.get();
		} catch (InterruptedException e) {
			throw ResponseError.newResponseError(function, botName, e);
		} catch (ExecutionException executionException) {
			if (executionException.getCause() instanceof ResponseError) {
				throw (ResponseError) executionException.getCause();
			} else {
				throw ResponseError.newResponseError(function, botName, executionException);
			}
		}
	}

	@Nullable
	public static <T> T get(@NotNull String tag, @NotNull String botName, CompletableFuture<T> action) throws ResponseError {
		try {
			return action.get();
		} catch (InterruptedException e) {
			throw ResponseError.newResponseError(tag, botName, e);
		} catch (ExecutionException executionException) {
			if (executionException.getCause() instanceof ResponseError) {
				throw (ResponseError) executionException.getCause();
			} else {
				throw ResponseError.newResponseError(tag, botName, executionException);
			}
		}
	}

	@NotNull
	public String getBotName() {
		return botName;
	}

	public int getErrorCode() {
		return code;
	}

	@NotNull
	public String getErrorMessage() {
		return message;
	}

	private static String functionToInlineString(Function function) {
		return function
				.toString()
				.replace("\n", " ")
				.replace("\t", "")
				.replace("  ", "")
				.replace(" = ", "=");
	}
}
