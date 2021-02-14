package it.tdlight.utils;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import io.vertx.reactivex.core.streams.Pipe;
import io.vertx.reactivex.core.streams.ReadStream;
import io.vertx.reactivex.core.streams.WriteStream;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.concurrency.future.CompletableFutureUtils;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class MonoUtils {

	private static final Logger logger = LoggerFactory.getLogger(MonoUtils.class);

	public static <T> Mono<T> notImplemented() {
		return Mono.fromCallable(() -> {
			throw new UnsupportedOperationException("Method not implemented");
		});
	}

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

	public static <T> Mono<T> fromBlockingMaybe(Callable<T> callable) {
		return Mono.fromCallable(callable).subscribeOn(Schedulers.boundedElastic());
	}

	public static Mono<Void> fromBlockingEmpty(EmptyCallable callable) {
		return Mono.<Void>fromCallable(() -> {
			callable.call();
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	public static <T> Mono<T> fromBlockingSingle(Callable<T> callable) {
		return fromBlockingMaybe(callable).single();
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
		mono.subscribe(cf::complete, cf::completeExceptionally, () -> cf.complete(null));
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
		return Mono.from(single.toFlowable());
	}

	public static <T> Mono<T> toMono(Maybe<T> single) {
		return Mono.from(single.toFlowable());
	}

	public static <T> Mono<T> toMono(Completable completable) {
		return Mono.from(completable.toFlowable());
	}

	public static <T> Completable toCompletable(Mono<T> s) {
		return Completable.fromPublisher(s);
	}

	public static Mono<Void> fromEmitResult(EmitResult emitResult) {
		return Mono.fromCallable(() -> {
			emitResult.orThrow();
			return null;
		});
	}

	public static Future<Void> fromEmitResultFuture(EmitResult emitResult) {
		if (emitResult.isSuccess()) {
			return Future.succeededFuture();
		} else {
			return Future.failedFuture(new EmissionException(emitResult));
		}
	}

	public static <T> Mono<Void> emitValue(One<T> sink, T value) {
		return Mono.defer(() -> fromEmitResult(sink.tryEmitValue(value)));
	}

	public static <T> Mono<Void> emitNext(Many<T> sink, T value) {
		return Mono.defer(() -> fromEmitResult(sink.tryEmitNext(value)));
	}

	public static <T> Mono<Void> emitComplete(Many<T> sink) {
		return Mono.defer(() -> fromEmitResult(sink.tryEmitComplete()));
	}

	public static <T> Mono<Void> emitEmpty(Empty<T> sink) {
		return Mono.defer(() -> fromEmitResult(sink.tryEmitEmpty()));
	}

	public static <T> Mono<Void> emitError(Empty<T> sink, Throwable value) {
		return Mono.defer(() -> fromEmitResult(sink.tryEmitError(value)));
	}

	public static <T> Future<Void> emitValueFuture(One<T> sink, T value) {
		return fromEmitResultFuture(sink.tryEmitValue(value));
	}

	public static <T> Future<Void> emitNextFuture(Many<T> sink, T value) {
		return fromEmitResultFuture(sink.tryEmitNext(value));
	}

	public static <T> Future<Void> emitCompleteFuture(Many<T> sink) {
		return fromEmitResultFuture(sink.tryEmitComplete());
	}

	public static <T> Future<Void> emitErrorFuture(Empty<T> sink, Throwable value) {
		return fromEmitResultFuture(sink.tryEmitError(value));
	}

	public static <T> Future<Void> emitEmptyFuture(Empty<T> sink) {
		return fromEmitResultFuture(sink.tryEmitEmpty());
	}

	public static <T> Mono<SinkRWStream<T>> unicastBackpressureSinkStream(Scheduler scheduler) {
		Many<T> sink = Sinks.many().unicast().onBackpressureBuffer();
		return asStream(sink, scheduler, null, null, 1);
	}

	/**
	 * Create a sink that can be written from a writeStream
	 */
	public static <T> Mono<SinkRWStream<T>> unicastBackpressureStream(Scheduler scheduler, int maxBackpressureQueueSize) {
		Queue<T> boundedQueue = Queues.<T>get(maxBackpressureQueueSize).get();
		var queueSize = Flux
				.interval(Duration.ZERO, Duration.ofMillis(500))
				.map(n -> boundedQueue.size());
		Empty<Void> termination = Sinks.empty();
		Many<T> sink = Sinks.many().unicast().onBackpressureBuffer(boundedQueue, termination::tryEmitEmpty);
		return asStream(sink, scheduler, queueSize, termination, maxBackpressureQueueSize);
	}

	public static <T> Mono<SinkRWStream<T>> unicastBackpressureErrorStream(Scheduler scheduler) {
		Many<T> sink = Sinks.many().unicast().onBackpressureError();
		return asStream(sink, scheduler, null, null, 1);
	}

	public static <T> Mono<SinkRWStream<T>> asStream(Many<T> sink,
			Scheduler scheduler,
			@Nullable Flux<Integer> backpressureSize,
			@Nullable Empty<Void> termination,
			int maxBackpressureQueueSize) {
		return SinkRWStream.create(sink, scheduler, backpressureSize, termination, maxBackpressureQueueSize);
	}

	private static Future<Void> toVertxFuture(Mono<Void> toTransform) {
		var promise = Promise.<Void>promise();
		toTransform.subscribeOn(Schedulers.parallel()).subscribe(next -> {}, promise::fail, promise::complete);
		return promise.future();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T> Mono<T> castVoid(Mono<Void> mono) {
		return (Mono) mono;
	}

	/**
	 * This method fails to guarantee that the consumer gets registered on all clusters before returning.
	 * Use fromConsumerAdvanced if you want better stability.
	 */
	@Deprecated
	public static <T> Flux<T> fromConsumerUnsafe(MessageConsumer<T> messageConsumer) {
		return Flux.<Message<T>>create(sink -> {
			messageConsumer.endHandler(e -> sink.complete());
			sink.onDispose(messageConsumer::unregister);
		})
				//.startWith(MonoUtils.castVoid(messageConsumer.rxCompletionHandler().as(MonoUtils::toMono)))
				.flatMapSequential(msg -> Mono
						.fromCallable(msg::body)
						.subscribeOn(Schedulers.parallel())
				);
	}

	public static <T> Mono<Tuple2<Mono<Void>, Flux<T>>> fromMessageConsumer(MessageConsumer<T> messageConsumer) {
		return fromReplyableMessageConsumer(messageConsumer)
				.map(tuple -> tuple.mapT2(msgs -> msgs.flatMapSequential(msg -> Mono
						.fromCallable(msg::body)
						.subscribeOn(Schedulers.parallel())))
				);
	}

	public static <T> Mono<Tuple2<Mono<Void>, Flux<Tuple2<Message<?>, T>>>> fromReplyableResolvedMessageConsumer(MessageConsumer<T> messageConsumer) {
		return fromReplyableMessageConsumer(messageConsumer)
				.map(tuple -> tuple.mapT2(msgs -> msgs.flatMapSequential(msg -> Mono
						.fromCallable(() -> Tuples.<Message<?>, T>of(msg, msg.body()))
						.subscribeOn(Schedulers.parallel())))
				);
	}

	public static <T> Mono<Tuple2<Mono<Void>, Flux<Message<T>>>> fromReplyableMessageConsumer(MessageConsumer<T> messageConsumer) {
		return Mono.<Tuple2<Mono<Void>, Flux<Message<T>>>>fromCallable(() -> {
			Many<Message<T>> messages = Sinks.many().unicast().onBackpressureError();
			Empty<Void> registrationRequested = Sinks.empty();
			Empty<Void> registrationCompletion = Sinks.empty();
			messageConsumer.endHandler(e -> {
				messages.tryEmitComplete();
				registrationCompletion.tryEmitEmpty();
			});
			messageConsumer.<Message<T>>handler(messages::tryEmitNext);

			Flux<Message<T>> dataFlux = Flux
					.<Message<T>>concatDelayError(
							messages.asFlux(),
							messageConsumer.rxUnregister().as(MonoUtils::toMono)
					)
					.doOnSubscribe(s -> registrationRequested.tryEmitEmpty());

			Mono<Void> registrationCompletionMono = Mono.empty()
					.doOnSubscribe(s -> registrationRequested.tryEmitEmpty())
					.then(registrationRequested.asMono())
					.doOnSuccess(s -> logger.trace("Subscribed to registration completion mono"))
					.doOnSuccess(s -> logger.trace("Waiting for consumer registration completion..."))
					.<Void>then(messageConsumer.rxCompletionHandler().as(MonoUtils::toMono))
					.doOnSuccess(s -> logger.trace("Consumer registered"))
					.share();
			return Tuples.of(registrationCompletionMono, dataFlux);
		}).subscribeOn(Schedulers.boundedElastic());
	}

	public static class SinkRWStream<T> implements io.vertx.core.streams.WriteStream<T>, io.vertx.core.streams.ReadStream<T> {

		private final Many<T> sink;
		private final Scheduler scheduler;
		private final Flux<Integer> backpressureSize;
		private final Empty<Void> termination;
		private Handler<Throwable> exceptionHandler = e -> {};
		private Handler<Void> drainHandler = h -> {};
		private final int maxBackpressureQueueSize;
		private volatile int writeQueueMaxSize;
		private volatile boolean writeQueueFull = false;

		private SinkRWStream(Many<T> sink,
				Scheduler scheduler,
				@Nullable Flux<Integer> backpressureSize,
				@Nullable Empty<Void> termination,
				int maxBackpressureQueueSize) {
			this.maxBackpressureQueueSize = maxBackpressureQueueSize;
			this.writeQueueMaxSize = this.maxBackpressureQueueSize;
			this.backpressureSize = backpressureSize;
			this.termination = termination;
			this.sink = sink;
			this.scheduler = scheduler;
		}

		public Mono<SinkRWStream<T>> initialize() {
			return Mono.fromCallable(() -> {
				if (backpressureSize != null) {
					AtomicBoolean drained = new AtomicBoolean(true);
					var drainSubscription = backpressureSize
							.subscribeOn(Schedulers.boundedElastic())
							.subscribe(size -> {
								writeQueueFull = size >= this.writeQueueMaxSize;

								boolean newDrained = size <= this.writeQueueMaxSize / 2;
								boolean oldDrained = drained.getAndSet(newDrained);
								if (newDrained && !oldDrained) {
									drainHandler.handle(null);
								}
							}, ex -> {
								exceptionHandler.handle(ex);
							}, () -> {
								if (!drained.get()) {
									drainHandler.handle(null);
								}
							});
					if (termination != null) {
						termination
								.asMono()
								.doOnTerminate(drainSubscription::dispose)
								.subscribeOn(Schedulers.boundedElastic())
								.subscribe();
					}
				}

				return this;
			}).subscribeOn(Schedulers.boundedElastic());
		}

		public static <T> Mono<SinkRWStream<T>> create(Many<T> sink,
				Scheduler scheduler,
				@Nullable Flux<Integer> backpressureSize,
				@Nullable Empty<Void> termination,
				int maxBackpressureQueueSize) {
			return new SinkRWStream<T>(sink, scheduler, backpressureSize, termination, maxBackpressureQueueSize).initialize();
		}

		public Flux<T> readAsFlux() {
			return sink.asFlux();
		}

		public ReactiveReactorReadStream<T> readAsStream() {
			return new ReactiveReactorReadStream<>(this);
		}

		public Many<T> writeAsSink() {
			return sink;
		}

		public ReactiveSinkWriteStream<T> writeAsStream() {
			return new ReactiveSinkWriteStream<>(this);
		}

		@Override
		public SinkRWStream<T> exceptionHandler(Handler<Throwable> handler) {
			exceptionHandler = handler;
			return this;
		}

		//
		// Read stream section
		//

		private Handler<Void> readEndHandler = v -> {};

		private Subscription readCoreSubscription;

		private final AtomicBoolean fetchMode = new AtomicBoolean(false);

		@Override
		public io.vertx.core.streams.ReadStream<T> handler(@io.vertx.codegen.annotations.Nullable Handler<T> handler) {
			sink.asFlux().subscribeOn(scheduler).subscribe(new CoreSubscriber<T>() {

				@Override
				public void onSubscribe(@NotNull Subscription s) {
					readCoreSubscription = s;
					if (!fetchMode.get()) {
						readCoreSubscription.request(1);
					}
				}

				@Override
				public void onNext(T t) {
					handler.handle(t);
					if (!fetchMode.get()) {
						readCoreSubscription.request(1);
					}
				}

				@Override
				public void onError(Throwable t) {
					exceptionHandler.handle(t);
				}

				@Override
				public void onComplete() {
					readEndHandler.handle(null);
				}
			});
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> pause() {
			fetchMode.set(true);
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> resume() {
			if (fetchMode.compareAndSet(true, false)) {
				readCoreSubscription.request(1);
			}
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> fetch(long amount) {
			if (fetchMode.get()) {
				if (amount > 0) {
					readCoreSubscription.request(amount);
				}
			}
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> endHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> endHandler) {
			this.readEndHandler = endHandler;
			return this;
		}

		//
		// Write stream section
		//

		@Override
		public Future<Void> write(T data) {
			return MonoUtils.emitNextFuture(sink, data);
		}

		@Override
		public void write(T data, Handler<AsyncResult<Void>> handler) {
			write(data).onComplete(handler);
		}

		@Override
		public void end(Handler<AsyncResult<Void>> handler) {
			MonoUtils.emitCompleteFuture(sink).onComplete(handler);
		}

		@Override
		public io.vertx.core.streams.WriteStream<T> setWriteQueueMaxSize(int maxSize) {
			if (maxSize <= maxBackpressureQueueSize) {
				this.writeQueueMaxSize = maxSize;
			} else {
				logger.error("Failed to set writeQueueMaxSize to " + maxSize + ", because it's bigger than the max backpressure queue size " + maxBackpressureQueueSize);
			}
			return this;
		}

		@Override
		public boolean writeQueueFull() {
			return writeQueueFull;
		}

		@Override
		public io.vertx.core.streams.WriteStream<T> drainHandler(@Nullable Handler<Void> handler) {
			this.drainHandler = handler;
			return this;
		}
	}

	public static class FluxReadStream<T> implements io.vertx.core.streams.ReadStream<T> {

		private final Flux<T> flux;
		private final Scheduler scheduler;
		private Handler<Throwable> exceptionHandler = e -> {};

		public FluxReadStream(Flux<T> flux, Scheduler scheduler) {
			this.flux = flux;
			this.scheduler = scheduler;
		}

		public Flux<T> readAsFlux() {
			return flux;
		}

		public ReactiveReactorReadStream<T> readAsStream() {
			return new ReactiveReactorReadStream<>(this);
		}

		@Override
		public FluxReadStream<T> exceptionHandler(Handler<Throwable> handler) {
			exceptionHandler = handler;
			return this;
		}

		//
		// Read stream section
		//

		private Handler<Void> readEndHandler = v -> {};

		private Subscription readCoreSubscription;

		private final AtomicBoolean fetchMode = new AtomicBoolean(false);

		@SuppressWarnings("DuplicatedCode")
		@Override
		public io.vertx.core.streams.ReadStream<T> handler(@io.vertx.codegen.annotations.Nullable Handler<T> handler) {
			flux.subscribeOn(scheduler).subscribe(new CoreSubscriber<T>() {

				@Override
				public void onSubscribe(@NotNull Subscription s) {
					readCoreSubscription = s;
					if (!fetchMode.get()) {
						readCoreSubscription.request(1);
					}
				}

				@Override
				public void onNext(T t) {
					handler.handle(t);
					if (!fetchMode.get()) {
						readCoreSubscription.request(1);
					}
				}

				@Override
				public void onError(Throwable t) {
					exceptionHandler.handle(t);
				}

				@Override
				public void onComplete() {
					readEndHandler.handle(null);
				}
			});
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> pause() {
			fetchMode.set(true);
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> resume() {
			if (fetchMode.compareAndSet(true, false)) {
				readCoreSubscription.request(1);
			}
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> fetch(long amount) {
			if (fetchMode.get()) {
				if (amount > 0) {
					readCoreSubscription.request(amount);
				}
			}
			return this;
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> endHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> endHandler) {
			this.readEndHandler = endHandler;
			return this;
		}
	}

	public static class ReactiveSinkWriteStream<T> implements WriteStream<T> {

		private final WriteStream<T> ws;

		public ReactiveSinkWriteStream(SinkRWStream<T> ws) {
			this.ws = WriteStream.newInstance(ws);
		}

		public io.vertx.core.streams.WriteStream<T> getDelegate() {
			//noinspection unchecked
			return ws.getDelegate();
		}

		@Override
		public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
			return ws.exceptionHandler(handler);
		}

		@Override
		public void write(T data, Handler<AsyncResult<Void>> handler) {
			ws.write(data, handler);
		}

		@Override
		public void write(T data) {
			ws.write(data);
		}

		@Override
		public Completable rxWrite(T data) {
			return ws.rxWrite(data);
		}

		@Override
		public void end(Handler<AsyncResult<Void>> handler) {
			ws.end(handler);
		}

		@Override
		public void end() {
			ws.end();
		}

		@Override
		public Completable rxEnd() {
			return ws.rxEnd();
		}

		@Override
		public void end(T data, Handler<AsyncResult<Void>> handler) {
			ws.end(data, handler);
		}

		@Override
		public void end(T data) {
			ws.end(data);
		}

		@Override
		public Completable rxEnd(T data) {
			return ws.rxEnd(data);
		}

		@Override
		public WriteStream<T> setWriteQueueMaxSize(int maxSize) {
			return ws.setWriteQueueMaxSize(maxSize);
		}

		@Override
		public boolean writeQueueFull() {
			return ws.writeQueueFull();
		}

		@Override
		public WriteStream<T> drainHandler(Handler<Void> handler) {
			return ws.drainHandler(handler);
		}
	}

	public static class ReactiveReactorReadStream<T> implements ReadStream<T> {

		private final ReadStream<T> rs;

		public ReactiveReactorReadStream(SinkRWStream<T> rws) {
			this.rs = ReadStream.newInstance(rws);
		}

		public ReactiveReactorReadStream(FluxReadStream<T> rs) {
			this.rs = ReadStream.newInstance(rs);
		}

		public ReactiveReactorReadStream(Flux<T> s, Scheduler scheduler) {
			this.rs = ReadStream.newInstance(new FluxReadStream<>(s, scheduler));
		}

		@Override
		public io.vertx.core.streams.ReadStream<T> getDelegate() {
			//noinspection unchecked
			return rs.getDelegate();
		}

		@Override
		public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
			return rs.exceptionHandler(handler);
		}

		@Override
		public ReadStream<T> handler(Handler<T> handler) {
			return rs.handler(handler);
		}

		@Override
		public ReadStream<T> pause() {
			return rs.pause();
		}

		@Override
		public ReadStream<T> resume() {
			return rs.resume();
		}

		@Override
		public ReadStream<T> fetch(long amount) {
			return rs.fetch(amount);
		}

		@Override
		public ReadStream<T> endHandler(Handler<Void> endHandler) {
			return rs.endHandler(endHandler);
		}

		@Override
		public Pipe<T> pipe() {
			return rs.pipe();
		}

		@Override
		public void pipeTo(WriteStream<T> dst, Handler<AsyncResult<Void>> handler) {
			rs.pipeTo(dst, handler);
		}

		@Override
		public void pipeTo(WriteStream<T> dst) {
			rs.pipeTo(dst);
		}

		@Override
		public Completable rxPipeTo(WriteStream<T> dst) {
			return rs.rxPipeTo(dst);
		}

		@Override
		public Observable<T> toObservable() {
			return rs.toObservable();
		}

		@Override
		public Flowable<T> toFlowable() {
			return rs.toFlowable();
		}
	}
}
