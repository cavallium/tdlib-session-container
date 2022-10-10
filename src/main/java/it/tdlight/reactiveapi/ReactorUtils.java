package it.tdlight.reactiveapi;

import it.cavallium.filequeue.DiskQueueToConsumer;
import it.tdlight.reactiveapi.rsocket.FileQueueUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

public class ReactorUtils {

	@SuppressWarnings("rawtypes")
	private static final WaitingSink WAITING_SINK = new WaitingSink<>();

	public static <V> Flux<V> subscribeOnce(Flux<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		});
	}

	public static <V> Flux<V> subscribeOnceUntilUnsubscribe(Flux<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		}).doFinally(s -> subscribed.set(false));
	}

	public static <V> Mono<V> subscribeOnce(Mono<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		});
	}

	public static <V> Mono<V> subscribeOnceUntilUnsubscribe(Mono<V> f) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return f.doOnSubscribe(s -> {
			if (!subscribed.compareAndSet(false, true)) {
				throw new UnsupportedOperationException("Can't subscribe more than once!");
			}
		}).doFinally(s -> subscribed.set(false));
	}

	public static <K> Flux<K> createLastestSubscriptionFlux(Flux<K> upstream, int maxBufferSize) {
		return upstream.transform(parent -> {
			AtomicReference<Subscription> subscriptionAtomicReference = new AtomicReference<>();
			AtomicReference<FluxSink<K>> prevEmitterRef = new AtomicReference<>();
			Deque<Signal<K>> queue = new ArrayDeque<>(maxBufferSize);

			return Flux.<K>create(emitter -> {
				var prevEmitter = prevEmitterRef.getAndSet(emitter);

				if (prevEmitter != null) {
					if (prevEmitter != WAITING_SINK) {
						prevEmitter.error(new CancellationException());
					}
					synchronized (queue) {
						Signal<K> next;
						while (!emitter.isCancelled() && (next = queue.peek()) != null) {
							if (next.isOnNext()) {
								queue.poll();
								var nextVal = next.get();
								assert nextVal != null;
								emitter.next(nextVal);
							} else if (next.isOnError()) {
								var throwable = next.getThrowable();
								assert throwable != null;
								emitter.error(throwable);
								break;
							} else if (next.isOnComplete()) {
								emitter.complete();
								break;
							} else {
								throw new UnsupportedOperationException();
							}
						}
					}
				} else {
					parent.subscribe(new CoreSubscriber<>() {
						@Override
						public void onSubscribe(@NotNull Subscription s) {
							subscriptionAtomicReference.set(s);
						}

						@Override
						public void onNext(K payload) {
							FluxSink<K> prevEmitter = prevEmitterRef.get();
							if (prevEmitter != WAITING_SINK) {
								prevEmitter.next(payload);
							} else {
								synchronized (queue) {
									queue.add(Signal.next(payload));
								}
							}
						}

						@Override
						public void onError(Throwable throwable) {
							FluxSink<K> prevEmitter = prevEmitterRef.get();
							synchronized (queue) {
								queue.add(Signal.error(throwable));
							}
							if (prevEmitter != WAITING_SINK) {
								prevEmitter.error(throwable);
							}
						}

						@Override
						public void onComplete() {
							FluxSink<K> prevEmitter = prevEmitterRef.get();
							synchronized (queue) {
								queue.add(Signal.complete());
							}
							if (prevEmitter != WAITING_SINK) {
								prevEmitter.complete();
							}
						}
					});
				}
				var s = subscriptionAtomicReference.get();
				emitter.onRequest(n -> {
					if (n > maxBufferSize) {
						emitter.error(new UnsupportedOperationException(
								"Requests count is bigger than max buffer size! " + n + " > " + maxBufferSize));
					} else {
						s.request(n);
					}
				});
				//noinspection unchecked
				emitter.onCancel(() -> prevEmitterRef.compareAndSet(emitter, WAITING_SINK));
				//noinspection unchecked
				emitter.onDispose(() -> prevEmitterRef.compareAndSet(emitter, WAITING_SINK));
			}, OverflowStrategy.BUFFER);
		});
	}

	public static <T> Function<Flux<T>, Flux<T>> onBackpressureBufferSubscribe(Path path,
			String name,
			boolean persistent,
			Serializer<T> serializer,
			Deserializer<T> deserializer) {
		return flux -> {
			AtomicReference<FluxSink<T>> ref = new AtomicReference<>();
			DiskQueueToConsumer<T> queue;
			try {
				var queuePath = path.resolve(".tdlib-queue");
				if (Files.notExists(queuePath)) {
					Files.createDirectories(queuePath);
				}
				queue = new DiskQueueToConsumer<>(queuePath.resolve(name + ".tape2"),
						!persistent,
						FileQueueUtils.convert(serializer),
						FileQueueUtils.convert(deserializer),
						signal -> {
							var sink = ref.get();
							if (sink != null && sink.requestedFromDownstream() > 0) {
								if (signal != null) {
									sink.next(signal);
								}
								return true;
							} else {
								return false;
							}
						}
				);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
			var disposable = flux
					.subscribeOn(Schedulers.parallel())
					.publishOn(Schedulers.boundedElastic())
					.subscribe(queue::add);
			queue.startQueue();
			return Flux.<T>create(sink -> {
				sink.onDispose(() -> {
					disposable.dispose();
					queue.close();
				});
				ref.set(sink);
				sink.onCancel(() -> ref.set(null));
			});
		};
	}

	public static <T> Function<Flux<T>, Flux<T>> onBackpressureBuffer(Path path,
			String name,
			boolean persistent,
			Serializer<T> serializer,
			Deserializer<T> deserializer) {
		return flux -> Flux.<T>create(sink -> {
			try {
				var queuePath = path.resolve(".tdlib-queue");
				if (Files.notExists(queuePath)) {
					Files.createDirectories(queuePath);
				}
				var queue = new DiskQueueToConsumer<>(queuePath.resolve(name + ".tape2"),
						!persistent,
						FileQueueUtils.convert(serializer),
						FileQueueUtils.convert(deserializer),
						signal -> {
							if (sink.requestedFromDownstream() > 0 && !sink.isCancelled()) {
								if (signal != null) {
									sink.next(signal);
								}
								return true;
							} else {
								return false;
							}
						}
				);
				sink.onDispose(queue::close);
				flux
						.subscribeOn(Schedulers.parallel())
						.publishOn(Schedulers.boundedElastic())
						.subscribe(new CoreSubscriber<T>() {
							@Override
							public void onSubscribe(@NotNull Subscription s) {
								sink.onCancel(s::cancel);
								s.request(Long.MAX_VALUE);
							}

							@Override
							public void onNext(T element) {
								if (!sink.isCancelled()) {
									queue.add(element);
								}
							}

							@Override
							public void onError(Throwable throwable) {
								sink.error(throwable);
							}

							@Override
							public void onComplete() {
							}
						});
				queue.startQueue();
			} catch (IOException ex) {
				sink.error(ex);
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private static class WaitingSink<T> implements FluxSink<T> {

		@Override
		public @NotNull FluxSink<T> next(@NotNull T t) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void complete() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void error(@NotNull Throwable e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull Context currentContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long requestedFromDownstream() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isCancelled() {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull FluxSink<T> onRequest(@NotNull LongConsumer consumer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull FluxSink<T> onCancel(@NotNull Disposable d) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull FluxSink<T> onDispose(@NotNull Disposable d) {
			throw new UnsupportedOperationException();
		}
	}
}
