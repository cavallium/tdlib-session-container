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
import it.tdlight.jni.TdApi.Chat;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.warp.commonutils.functional.IOConsumer;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
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

public class MonoUtils {

	private static final Logger logger = LogManager.getLogger(MonoUtils.class);

	public static <T> Mono<T> notImplemented() {
		return Mono.fromCallable(() -> {
			throw new UnsupportedOperationException("Method not implemented");
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

	public static <R extends TdApi.Object> void orElseThrowFuture(TdResult<R> value, SynchronousSink<CompletableFuture<R>> sink) {
		if (value.succeeded()) {
			sink.next(CompletableFuture.completedFuture(value.result()));
		} else {
			sink.next(CompletableFuture.failedFuture(new TdError(value.cause().code, value.cause().message)));
		}
	}

	public static <T extends TdApi.Object> void orElseThrow(TdResult<T> value, SynchronousSink<T> sink) {
		if (value.succeeded()) {
			sink.next(value.result());
		} else {
			sink.error(new TdError(value.cause().code, value.cause().message));
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

	public static <T> Mono<T> toMono(Future<T> future) {
		return Mono.create(sink -> future.onComplete(result -> {
			if (result.succeeded()) {
				sink.success(result.result());
			} else {
				sink.error(result.cause());
			}
		}));
	}

	@NotNull
	public static <T> Mono<T> toMono(Single<T> single) {
		return Mono.from(single.toFlowable());
	}

	@NotNull
	public static <T> Mono<T> toMono(Maybe<T> single) {
		return Mono.from(single.toFlowable());
	}

	@NotNull
	public static <T> Mono<T> toMono(Completable completable) {
		return Mono.from(completable.toFlowable());
	}

	public static <T> Completable toCompletable(Mono<T> s) {
		return Completable.fromPublisher(s);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T> Mono<T> castVoid(Mono<Void> mono) {
		return (Mono) mono;
	}

	public static <T> Flux<T> fromMessageConsumer(Mono<Void> onRegistered, MessageConsumer<T> messageConsumer) {
		return fromReplyableMessageConsumer(onRegistered, messageConsumer).map(Message::body);
	}

	public static <T> Flux<Message<T>> fromReplyableMessageConsumer(Mono<Void> onRegistered,
			MessageConsumer<T> messageConsumer) {
		Mono<Void> endMono = Mono.create(sink -> {
			AtomicBoolean alreadyRequested = new AtomicBoolean();
			sink.onRequest(n -> {
				if (n > 0 && alreadyRequested.compareAndSet(false, true)) {
					messageConsumer.endHandler(e -> sink.success());
				}
			});
		});

		Mono<MessageConsumer<T>> registrationCompletionMono = Mono
				.fromRunnable(() -> logger.trace("Waiting for consumer registration completion..."))
				.<Void>then(messageConsumer.rxCompletionHandler().as(MonoUtils::toMono))
				.doOnSuccess(s -> logger.trace("Consumer registered"))
				.then(onRegistered)
				.thenReturn(messageConsumer);

		messageConsumer.handler(s -> {
			throw new IllegalStateException("Subscriber still didn't request any value!");
		});

		Flux<Message<T>> dataFlux = Flux
				.push(sink -> sink.onRequest(n -> messageConsumer.handler(sink::next)), OverflowStrategy.ERROR);

		Mono<Void> disposeMono = messageConsumer
				.rxUnregister()
				.as(MonoUtils::<Message<T>>toMono)
				.doOnSuccess(s -> logger.trace("Unregistered message consumer"))
				.then();

		return Flux
				.usingWhen(registrationCompletionMono, msgCons -> dataFlux, msgCons -> disposeMono)
				.takeUntilOther(endMono);
	}

	public static Scheduler newBoundedSingle(String name) {
		return newBoundedSingle(name, false);
	}

	public static Scheduler newBoundedSingle(String name, boolean daemon) {
		return Schedulers.newBoundedElastic(1,
				Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
				name,
				Integer.MAX_VALUE,
				daemon
		);
	}

	public static <R> Mono<Optional<R>> toOptional(Mono<R> mono) {
		return mono.map(Optional::of).defaultIfEmpty(Optional.empty());
	}

	public static <T> Mono<Boolean> isSet(Mono<T> mono) {
		return mono
				.map(res -> true)
				.defaultIfEmpty(false);
	}

	@FunctionalInterface
	public interface VoidCallable {
		void call() throws Exception;
	}

	public static Mono<?> fromVoidCallable(VoidCallable callable) {
		return Mono.fromCallable(() -> {
			callable.call();
			return null;
		});
	}
}
