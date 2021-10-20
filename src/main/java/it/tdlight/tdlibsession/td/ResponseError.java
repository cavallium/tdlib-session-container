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

	private ResponseError(@NotNull Function<?> function, @NotNull String botName, @NotNull TdApi.Error tdError, @Nullable Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + functionToInlineString(function) + "': " + tdError.code + " " + tdError.message, cause);
		this.botName = botName;
		this.tag = functionToInlineString(function);
		this.code = tdError.code;
		this.message = tdError.message;
	}

	private ResponseError(@NotNull String tag, @NotNull String botName, @NotNull TdApi.Error tdError, @Nullable Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + tag + "': " + tdError.code + " " + tdError.message, cause);
		this.botName = botName;
		this.tag = tag;
		this.code = tdError.code;
		this.message = tdError.message;
	}

	private ResponseError(@NotNull Function<?> function, @NotNull String botName, @Nullable Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + functionToInlineString(function) + "': " + (cause == null ? null : cause.getMessage()), cause);
		this.botName = botName;
		this.tag = functionToInlineString(function);
		this.code = 500;
		this.message = (cause == null ? "" : (cause.getMessage() == null ? "" : cause.getMessage()));
	}

	private ResponseError(@NotNull String tag, @NotNull String botName, @Nullable Throwable cause) {
		super("Bot '" + botName + "' failed the request '" + tag + "': " + (cause == null ? null : cause.getMessage()), cause);
		this.botName = botName;
		this.tag = tag;
		this.code = 500;
		this.message = (cause == null ? "" : (cause.getMessage() == null ? "" : cause.getMessage()));
	}

	public static ResponseError newResponseError(@NotNull Function<?> function,
			@NotNull String botName,
			@NotNull TdApi.Error tdError,
			@Nullable Throwable cause) {
		return new ResponseError(function, botName, tdError, cause);
	}

	public static ResponseError newResponseError(@NotNull String tag,
			@NotNull String botName,
			@NotNull TdApi.Error tdError,
			@Nullable Throwable cause) {
		return new ResponseError(tag, botName, tdError, cause);
	}

	public static ResponseError newResponseError(@NotNull String tag,
			@NotNull String botName,
			@NotNull TdApi.Error tdError,
			@Nullable TdError cause) {
		return new ResponseError(tag, botName, tdError, cause);
	}

	public static ResponseError newResponseError(@NotNull Function<?> function,
			@NotNull String botName,
			@Nullable Throwable cause) {
		return new ResponseError(function, botName, cause);
	}

	public static ResponseError newResponseError(@NotNull String tag,
			@NotNull String botName,
			@Nullable Throwable cause) {
		return new ResponseError(tag, botName, cause);
	}

	@Nullable
	public static <T> T get(@NotNull Function<?> function, @NotNull String botName, CompletableFuture<T> action) throws ResponseError {
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

	public String getTag() {
		return tag;
	}

	@NotNull
	public String getErrorMessage() {
		return message;
	}

	private static String functionToInlineString(Function<?> function) {
		return function
				.toString()
				.replace("\n", " ")
				.replace("\t", "")
				.replace("  ", "")
				.replace(" = ", "=")
				.trim();
	}

	@Override
	public String toString() {
		return getMessage();
	}
}
