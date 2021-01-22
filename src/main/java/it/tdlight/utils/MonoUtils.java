package it.tdlight.utils;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.concurrency.future.CompletableFutureUtils;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.Context;

public class MonoUtils {

	private static final Logger logger = LoggerFactory.getLogger(MonoUtils.class);

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

	public static <R extends TdApi.Object> Mono<R> orElseThrow(TdResult<R> value) {
		if (value.succeeded()) {
			return Mono.just(value.result());
		} else {
			return Mono.error(new TdError(value.cause().code, value.cause().message));
		}
	}

	public static <T extends TdApi.Object> Mono<Void> thenOrError(Mono<TdResult<T>> optionalMono) {
		return optionalMono.handle((optional, sink) -> {
			if (optional.succeeded()) {
				sink.complete();
			} else {
				sink.error(new TdError(optional.cause().code, optional.cause().message));
			}
		});
	}

	public static <T extends TdApi.Object> Mono<Void> thenOrLogSkipError(Mono<TdResult<T>> optionalMono) {
		return optionalMono.handle((optional, sink) -> {
			if (optional.failed()) {
				logger.error("Received TDLib error: {}", optional.cause());
			}
			sink.complete();
		});
	}

	public static <T extends TdApi.Object> Mono<T> orElseLogSkipError(TdResult<T> optional) {
		if (optional.failed()) {
			logger.error("Received TDLib error: {}", optional.cause());
			return Mono.empty();
		}
		return Mono.just(optional.result());
	}

	public static <T extends TdApi.Object> Mono<Void> thenOrLogRepeatError(Supplier<? extends Mono<TdResult<T>>> optionalMono) {
		return Mono.defer(() -> optionalMono.get().handle((TdResult<T> optional, SynchronousSink<Void> sink) -> {
			if (optional.succeeded()) {
				sink.complete();
			} else {
				logger.error("Received TDLib error: {}", optional.cause());
				sink.error(new TdError(optional.cause().code, optional.cause().message));
			}
		})).retry();
	}

	public static <T> Mono<T> fromFuture(CompletableFuture<T> future) {
		return Mono.create(sink -> {
			future.whenComplete((result, error) -> {
				if (error != null) {
					sink.error(error);
				} else if (result != null) {
					sink.success(result);
				} else {
					sink.success();
				}
			});
		});
	}

	public static <T> Mono<T> fromFuture(Supplier<CompletableFuture<T>> future) {
		return Mono.create(sink -> {
			CompletableFutureUtils.getCompletableFuture(future).whenComplete((result, error) -> {
				if (error != null) {
					sink.error(error.getCause());
				} else if (result != null) {
					sink.success(result);
				} else {
					sink.success();
				}
			});
		});
	}

	public static <T extends Object> CompletableFuture<T> toFuture(Mono<T> mono) {
		var cf = new CompletableFuture<T>();
		mono.subscribe(value -> {
			cf.complete(value);
		}, ex -> {
			cf.completeExceptionally(ex);
		}, () -> cf.complete(null));
		return cf;
	}

	public static <T> Mono<T> toMono(Future<T> future) {
		return Mono.<T>create(sink -> future.onComplete(result -> {
			if (result.succeeded()) {
				sink.success(result.result());
			} else {
				sink.error(result.cause());
			}
		}));
	}

	public static <T> Mono<T> toMono(Single<T> single) {
		return Mono.fromDirect(single.toFlowable());
	}

	public static <T> Mono<T> toMono(Maybe<T> single) {
		return Mono.fromDirect(single.toFlowable());
	}

	public static <T> Mono<T> toMono(Completable completable) {
		return Mono.fromDirect(completable.toFlowable());
	}

	public static <T> Completable toCompletable(Mono<T> s) {
		return Completable.fromPublisher(s);
	}
}
