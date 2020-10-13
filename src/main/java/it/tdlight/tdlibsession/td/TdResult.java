package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates the result of an asynchronous operation.
 * <p>
 * Many operations in Vert.x APIs provide results back by passing an instance of this in a {@link io.vertx.core.Handler}.
 * <p>
 * The result can either have failed or succeeded.
 * <p>
 * If it failed then the cause of the failure is available with {@link #cause}.
 * <p>
 * If it succeeded then the actual result is available with {@link #result}
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public interface TdResult<T extends TdApi.Object> {

	/**
	 * The result of the operation. This will be null if the operation failed.
	 *
	 * @return the result or null if the operation failed.
	 */
	T result();

	/**
	 * The result of the operation. This will throw CompletionException if the operation failed.
	 *
	 * @return the result.
	 */
	T orElseThrow() throws CompletionException;

	/**
	 * A TdApi.Error describing failure. This will be null if the operation succeeded.
	 *
	 * @return the cause or null if the operation succeeded.
	 */
	TdApi.Error cause();

	/**
	 * Did it succeed?
	 *
	 * @return true if it succeded or false otherwise
	 */
	boolean succeeded();

	/**
	 * Did it fail?
	 *
	 * @return true if it failed or false otherwise
	 */
	boolean failed();

	/**
	 * Apply a {@code mapper} function on this async result.<p>
	 *
	 * The {@code mapper} is called with the completed value and this mapper returns a value. This value will complete the result returned by this method call.<p>
	 *
	 * When this async result is failed, the failure will be propagated to the returned async result and the {@code mapper} will not be called.
	 *
	 * @param mapper the mapper function
	 * @return the mapped async result
	 */
	default <U extends TdApi.Object> TdResult<U> map(Function<T, U> mapper) {
		if (mapper == null) {
			throw new NullPointerException();
		}
		return new TdResult<U>() {
			@Override
			public U result() {
				if (succeeded()) {
					return mapper.apply(TdResult.this.result());
				} else {
					return null;
				}
			}

			@Override
			public U orElseThrow() throws CompletionException {
				if (succeeded()) {
					return mapper.apply(TdResult.this.orElseThrow());
				} else {
					return null;
				}
			}

			@Override
			public TdApi.Error cause() {
				return TdResult.this.cause();
			}

			@Override
			public boolean succeeded() {
				return TdResult.this.succeeded();
			}

			@Override
			public boolean failed() {
				return TdResult.this.failed();
			}
		};
	}

	/**
	 * Map the result of this async result to a specific {@code value}.<p>
	 *
	 * When this async result succeeds, this {@code value} will succeeed the async result returned by this method call.<p>
	 *
	 * When this async result fails, the failure will be propagated to the returned async result.
	 *
	 * @param value the value that eventually completes the mapped async result
	 * @return the mapped async result
	 */
	default <V extends TdApi.Object> TdResult<V> map(V value) {
		return map((Function<T, V>) t -> value);
	}

	/**
	 * Map the result of this async result to {@code null}.<p>
	 *
	 * This is a convenience for {@code TdResult.map((T) null)} or {@code TdResult.map((Void) null)}.<p>
	 *
	 * When this async result succeeds, {@code null} will succeeed the async result returned by this method call.<p>
	 *
	 * When this async result fails, the failure will be propagated to the returned async result.
	 *
	 * @return the mapped async result
	 */
	default <V extends TdApi.Object> TdResult<V> mapEmpty() {
		return map((V)null);
	}

	/**
	 * Apply a {@code mapper} function on this async result.<p>
	 *
	 * The {@code mapper} is called with the failure and this mapper returns a value. This value will complete the result returned by this method call.<p>
	 *
	 * When this async result is succeeded, the value will be propagated to the returned async result and the {@code mapper} will not be called.
	 *
	 * @param mapper the mapper function
	 * @return the mapped async result
	 */
	default TdResult<T> otherwise(Function<TdApi.Error, T> mapper) {
		if (mapper == null) {
			throw new NullPointerException();
		}
		return new TdResult<T>() {
			@Override
			public T result() {
				if (TdResult.this.succeeded()) {
					return TdResult.this.result();
				} else if (TdResult.this.failed()) {
					return mapper.apply(TdResult.this.cause());
				} else {
					return null;
				}
			}
			@Override
			public T orElseThrow() {
				if (TdResult.this.succeeded()) {
					return TdResult.this.orElseThrow();
				} else if (TdResult.this.failed()) {
					return mapper.apply(TdResult.this.cause());
				} else {
					return null;
				}
			}

			@Override
			public TdApi.Error cause() {
				return null;
			}

			@Override
			public boolean succeeded() {
				return TdResult.this.succeeded() || TdResult.this.failed();
			}

			@Override
			public boolean failed() {
				return false;
			}
		};
	}

	/**
	 * Map the failure of this async result to a specific {@code value}.<p>
	 *
	 * When this async result fails, this {@code value} will succeeed the async result returned by this method call.<p>
	 *
	 * When this async succeeds, the result will be propagated to the returned async result.
	 *
	 * @param value the value that eventually completes the mapped async result
	 * @return the mapped async result
	 */
	default TdResult<T> otherwise(T value) {
		return otherwise(err -> value);
	}

	/**
	 * Map the failure of this async result to {@code null}.<p>
	 *
	 * This is a convenience for {@code TdResult.otherwise((T) null)}.<p>
	 *
	 * When this async result fails, the {@code null} will succeeed the async result returned by this method call.<p>
	 *
	 * When this async succeeds, the result will be propagated to the returned async result.
	 *
	 * @return the mapped async result
	 */
	default TdResult<T> otherwiseEmpty() {
		return otherwise(err -> null);
	}

	static <T extends TdApi.Object> TdResult<T> succeeded(@NotNull T value) {
		return new TdResultImpl<T>(value, null);
	}

	static <T extends TdApi.Object> TdResult<T> failed(@NotNull TdApi.Error error) {
		return new TdResultImpl<T>(null, error);
	}

	static <T extends TdApi.Object> TdResult<T> of(@NotNull TdApi.Object resultOrError) {
		if (resultOrError.getConstructor() == TdApi.Error.CONSTRUCTOR) {
			return failed((TdApi.Error) resultOrError);
		} else {
			//noinspection unchecked
			return succeeded((T) resultOrError);
		}
	}

	class TdResultImpl<U extends TdApi.Object> implements TdResult<U> {

		private final U value;
		private final Error error;

		public TdResultImpl(U value, Error error) {
			this.value = value;
			this.error = error;

			assert (value == null) != (error == null);
		}

		@Override
		public U result() {
			return value;
		}

		@Override
		public U orElseThrow() {
			if (error != null) {
				throw new TdError(error.code, error.message);
			}
			return value;
		}

		@Override
		public Error cause() {
			return error;
		}

		@Override
		public boolean succeeded() {
			return value != null;
		}

		@Override
		public boolean failed() {
			return error != null;
		}
	}
}
