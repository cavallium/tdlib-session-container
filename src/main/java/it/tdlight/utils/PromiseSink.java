package it.tdlight.utils;

import io.vertx.core.Promise;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.Context;

public abstract class PromiseSink<T> implements SynchronousSink<T> {

	private final Promise<T> promise;

	private PromiseSink(Promise<T> promise) {
		this.promise = promise;
	}

	public static <K> PromiseSink<K> of(Context context, Promise<K> promise) {
		return new PromiseSinkImpl<>(promise, context);
	}

	@Override
	public void complete() {
		promise.complete();
	}

	@Override
	public void error(@NotNull Throwable error) {
		promise.fail(error);
	}

	@Override
	public void next(@NotNull T value) {
		promise.complete(value);
	}

	private static class PromiseSinkImpl<K> extends PromiseSink<K> {

		private final Context context;

		public PromiseSinkImpl(Promise<K> promise, Context context) {
			super(promise);
			this.context = context;
		}

		@Override
		public @NotNull Context currentContext() {
			return context;
		}
	}
}
