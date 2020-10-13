package it.tdlight.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import org.warp.commonutils.concurrency.future.CompletableFutureUtils;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.Context;

public class MonoUtils {

	public static <T> Handler<AsyncResult<T>> toHandler(SynchronousSink<T> sink) {
		return event -> {
			if (event.succeeded()) {
				if (event.result() == null) {
					sink.complete();
				} else {
					sink.next(Objects.requireNonNull(event.result()));
				}
			} else {
				sink.error(event.cause());
			}
		};
	}

	public static <T> Handler<AsyncResult<T>> toHandler(MonoSink<T> sink) {
		return event -> {
			if (event.succeeded()) {
				if (event.result() == null) {
					sink.success();
				} else {
					sink.success(Objects.requireNonNull(event.result()));
				}
			} else {
				sink.error(event.cause());
			}
		};
	}

	public static <T> SynchronousSink<T> toSink(Context context, Promise<T> promise) {
		return PromiseSink.of(context, promise);
	}

	public static <T, R> BiConsumer<? super T, SynchronousSink<R>> executeBlockingSink(Vertx vertx, BiConsumer<? super T, SynchronousSink<R>> handler) {
		return (value, sink) -> {
			vertx.executeBlocking((Promise<R> finished) -> {
				handler.accept(value, PromiseSink.of(sink.currentContext(), finished));
			}, toHandler(sink));
		};
	}

	public static <T> Mono<T> executeBlocking(Vertx vertx, Consumer<SynchronousSink<T>> action) {
		return Mono.create((MonoSink<T> sink) -> {
			vertx.executeBlocking((Promise<T> finished) -> {
				action.accept(toSink(sink.currentContext(), finished));
			}, toHandler(sink));
		});
	}

	public static <T> Mono<T> executeAsFuture(Consumer<Handler<AsyncResult<T>>> action) {
		return Mono.<T>fromFuture(() -> {
			return CompletableFutureUtils.getCompletableFuture(() -> {
				var resultFuture = new CompletableFuture<T>();
				action.accept(handler -> {
					if (handler.failed()) {
						resultFuture.completeExceptionally(handler.cause());
					} else {
						resultFuture.complete(handler.result());
					}
				});
				return resultFuture;
			});
		});
	}

	public static <T> CoreSubscriber<? super T> toSubscriber(Promise<T> promise) {
		return new CoreSubscriber<T>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(1);
			}

			@Override
			public void onNext(T t) {
				promise.complete(t);
			}

			@Override
			public void onError(Throwable t) {
				promise.fail(t);
			}

			@Override
			public void onComplete() {
				promise.tryComplete();
			}
		};
	}

	public static <R extends TdApi.Object> void orElseThrowFuture(TdResult<R> value, SynchronousSink<CompletableFuture<R>> sink) {
		if (value.succeeded()) {
			sink.next(CompletableFuture.completedFuture(value.result()));
		} else {
			sink.next(CompletableFuture.failedFuture(new TdError(value.cause().code, value.cause().message)));
		}
	}

	public static <R extends TdApi.Object> void orElseThrow(TdResult<R> value, SynchronousSink<R> sink) {
		if (value.succeeded()) {
			sink.next(value.result());
		} else {
			sink.complete();
			//sink.error(new TdError(value.cause().code, value.cause().message));
		}
	}
}
