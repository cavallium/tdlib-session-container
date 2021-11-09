package it.tdlight.tdlibsession.td.middle.client;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.middle.TdResultMessage;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.EndSessionMessage;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.StartSessionMessage;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.utils.BinlogAsyncFile;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import java.net.ConnectException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.warp.commonutils.locks.LockUtils;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;

public class AsyncTdMiddleEventBusClient implements AsyncTdMiddle {

	private Logger logger;

	public static final byte[] EMPTY = new byte[0];

	private final TdClusterManager cluster;
	private final DeliveryOptions deliveryOptions;
	private final DeliveryOptions deliveryOptionsWithTimeout;
	private final DeliveryOptions pingDeliveryOptions;

	private final AtomicReference<BinlogAsyncFile> binlog = new AtomicReference<>();

	private final AtomicReference<Disposable> pinger = new AtomicReference<>();

	private final AtomicReference<MessageConsumer<TdResultList>> updates = new AtomicReference<>();
	// This will only result in a successful completion, never completes in other ways
	private final Empty<Void> updatesStreamEnd = Sinks.empty();
	// This will only result in a crash, never completes in other ways
	private final AtomicReference<Throwable> crash = new AtomicReference<>();
	// This will only result in a successful completion, never completes in other ways
	private final Empty<Void> pingFail = Sinks.empty();

	private long botId;
	private String botAddress;
	private String botAlias;
	private boolean local;

	public AsyncTdMiddleEventBusClient(TdClusterManager clusterManager) {
		this.logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusClient.class);
		this.cluster = clusterManager;
		this.deliveryOptions = cluster.newDeliveryOpts().setLocalOnly(local);
		this.deliveryOptionsWithTimeout = cluster.newDeliveryOpts().setLocalOnly(local).setSendTimeout(30000);
		this.pingDeliveryOptions = cluster.newDeliveryOpts().setLocalOnly(local).setSendTimeout(60000);
	}

	private Mono<AsyncTdMiddleEventBusClient> initializeEb() {
		return Mono.just(this);
	}

	@Override
	public Mono<Void> initialize() {
		// Do nothing here.
		return Mono.empty();
	}

	public static Mono<AsyncTdMiddle> getAndDeployInstance(TdClusterManager clusterManager,
			long botId,
			String botAlias,
			boolean local,
			JsonObject implementationDetails,
			Path binlogsArchiveDirectory) {
		return new AsyncTdMiddleEventBusClient(clusterManager)
				.initializeEb()
				.flatMap(instance -> retrieveBinlog(clusterManager.getVertx(), binlogsArchiveDirectory, botId)
						.flatMap(binlog -> binlog
								.getLastModifiedTime()
								.filter(modTime -> modTime == 0)
								.doOnNext(v -> LoggerFactory
										.getLogger(AsyncTdMiddleEventBusClient.class)
										.error("Can't retrieve binlog of bot " + botId + " " + botAlias + ". Creating a new one..."))
								.thenReturn(binlog)).<AsyncTdMiddle>flatMap(binlog -> instance
								.start(botId, botAlias, local, implementationDetails, binlog)
								.thenReturn(instance)
						)
						.single()
				)
				.single();
	}

	/**
	 *
	 * @return optional result
	 */
	public static Mono<BinlogAsyncFile> retrieveBinlog(Vertx vertx, Path binlogsArchiveDirectory, long botId) {
		return BinlogUtils.retrieveBinlog(vertx.fileSystem(), binlogsArchiveDirectory.resolve(botId + ".binlog"));
	}

	private Mono<Void> saveBinlog(Buffer data) {
		return Mono.fromSupplier(this.binlog::get).flatMap(binlog -> BinlogUtils.saveBinlog(binlog, data));
	}

	public Mono<Void> start(long botId,
			String botAlias,
			boolean local,
			JsonObject implementationDetails,
			BinlogAsyncFile binlog) {
		this.botId = botId;
		this.botAlias = botAlias;
		this.botAddress = "bots.bot." + this.botId;
		this.local = local;
		this.logger = LoggerFactory.getLogger(this.botId + " " + botAlias);

		return Mono
				.fromRunnable(() -> this.binlog.set(binlog))
				.then(binlog.getLastModifiedTime())
				.zipWith(binlog.readFully().map(Buffer::getDelegate))
				.single()
				.flatMap(tuple -> {
					var binlogLastModifiedTime = tuple.getT1();
					var binlogData = tuple.getT2();

					var msg = new StartSessionMessage(this.botId,
							this.botAlias,
							Buffer.newInstance(binlogData),
							binlogLastModifiedTime,
							implementationDetails
					);

					Mono<Void> startBotRequest;

					if (local) {
						startBotRequest = Mono.empty();
					} else {
						startBotRequest = cluster
								.getEventBus()
								.<byte[]>rxRequest("bots.start-bot", msg)
								.to(RxJava2Adapter::singleToMono)
								.doOnSuccess(s -> logger.trace("bots.start-bot returned successfully"))
								.doFirst(() -> logger.trace("Requesting bots.start-bot"))
								.onErrorMap(ex -> {
									if (ex instanceof ReplyException) {
										if (((ReplyException) ex).failureType() == ReplyFailure.NO_HANDLERS) {
											return new NoClustersAvailableException("Can't start bot "
													+ botId + " " + botAlias);
										}
									}
									return ex;
								})
								.then()
								.subscribeOn(Schedulers.boundedElastic());
					}

					return setupUpdatesListener().then(startBotRequest).then(setupPing());
				});
	}

	private Mono<Void> setupPing() {
		// Disable ping on local servers
		if (local) {
			return Mono.empty();
		}

		var pingRequest = cluster.getEventBus()
				.<byte[]>rxRequest(botAddress + ".ping", EMPTY, pingDeliveryOptions)
				.to(RxJava2Adapter::singleToMono)
				.doFirst(() -> logger.trace("Requesting ping..."));

		return Mono
				.fromRunnable(() -> pinger.set(pingRequest
					.flatMap(msg -> Mono.fromCallable(msg::body).subscribeOn(Schedulers.boundedElastic()))
					.repeatWhen(l -> l.delayElements(Duration.ofSeconds(10)).takeWhile(x -> true))
					.doOnNext(s -> logger.trace("PING"))
					.then()
					.onErrorResume(ex -> {
						logger.warn("Ping failed: {}", ex.getMessage());
						return Mono.empty();
					})
					.doOnNext(s -> logger.debug("END PING"))
					.then(Mono.fromRunnable(() -> {
						while (this.pingFail.tryEmitEmpty() == EmitResult.FAIL_NON_SERIALIZED) {
							// 10ms
							LockSupport.parkNanos(10000000);
						}
					}).subscribeOn(Schedulers.boundedElastic()))
					.subscribeOn(Schedulers.parallel())
					.subscribe())
				)
				.then()
				.doFirst(() -> logger.trace("Setting up ping"))
				.doOnSuccess(s -> logger.trace("Ping setup success"))
				.subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<Void> setupUpdatesListener() {
		return Mono
				.fromRunnable(() -> logger.trace("Setting up updates listener..."))
				.then(Mono.<MessageConsumer<TdResultList>>fromSupplier(() -> MessageConsumer
						.newInstance(cluster.getEventBus().<TdResultList>consumer(botAddress + ".updates")
								.setMaxBufferedMessages(5000)
								.getDelegate()
						))
				)
				.flatMap(updateConsumer -> {
					// Return when the registration of all the consumers has been done across the cluster
					return Mono
							.fromRunnable(() -> logger.trace("Emitting updates flux to sink"))
							.then(Mono.fromRunnable(() -> {
								var previous = this.updates.getAndSet(updateConsumer);
								if (previous != null) {
									logger.error("Already subscribed a consumer to the updates");
								}
							}))
							.doOnSuccess(s -> logger.trace("Emitted updates flux to sink"))
							.doOnSuccess(s -> logger.trace("Waiting to register update consumer across the cluster"))
							.doOnSuccess(s -> logger.trace("Registered update consumer across the cluster"));
				})
				.doOnSuccess(s ->logger.trace("Set up updates listener"))
				.then();
	}

	@SuppressWarnings("Convert2MethodRef")
	@Override
	public Flux<TdApi.Object> receive() {
		// Here the updates will be received

		return Mono
				.fromRunnable(() -> logger.trace("Called receive() from parent"))
				.then(Mono.fromCallable(() -> updates.get()))
				.doOnSuccess(s -> logger.trace("Registering updates flux"))
				.flatMapMany(updatesMessageConsumer -> MonoUtils.fromMessageConsumer(Mono
						.empty()
						.doOnSuccess(s -> logger.trace("Sending ready-to-receive"))
						.then(cluster.getEventBus().<byte[]>rxRequest(botAddress + ".ready-to-receive",
								EMPTY,
								deliveryOptionsWithTimeout
						).to(RxJava2Adapter::singleToMono))
						.doOnSuccess(s -> logger.trace("Sent ready-to-receive, received reply"))
						.doOnSuccess(s -> logger.trace("About to read updates flux"))
						.then(), updatesMessageConsumer)
				)
				.takeUntilOther(pingFail.asMono())
				.takeUntil(a -> a.succeeded() && a.value().stream().anyMatch(item -> {
					if (item.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR) {
						return ((UpdateAuthorizationState) item).authorizationState.getConstructor()
								== AuthorizationStateClosed.CONSTRUCTOR;
					}
					return false;
				}))
				.flatMapSequential(updates -> {
					if (updates.succeeded()) {
						return Flux.fromIterable(updates.value());
					} else {
						return Mono.fromCallable(() -> TdResult.failed(updates.error()).orElseThrow());
					}
				})
				.concatMap(update -> interceptUpdate(update))
				// Redirect errors to crash sink
				.doOnError(error -> crash.compareAndSet(null, error))
				.onErrorResume(ex -> {
					logger.trace("Absorbing the error, the error has been published using the crash sink", ex);
					return Mono.empty();
				})
				.doOnCancel(() -> {
				})
				.doFinally(s -> {
					var pinger = this.pinger.get();
					if (pinger != null) {
						pinger.dispose();
					}
					updatesStreamEnd.tryEmitEmpty();
				});
	}

	private Mono<TdApi.Object> interceptUpdate(Object update) {
		logger.trace("Received update {}", update.getClass().getSimpleName());
		if (update.getConstructor() == TdApi.UpdateAuthorizationState.CONSTRUCTOR) {
			var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
			if (updateAuthorizationState.authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR) {
				logger.info("Received AuthorizationStateClosed from tdlib");

				var pinger = this.pinger.get();
				if (pinger != null) {
					pinger.dispose();
				}

				return cluster.getEventBus()
						.<EndSessionMessage>rxRequest(this.botAddress + ".read-binlog", EMPTY)
						.to(RxJava2Adapter::singleToMono)
						.mapNotNull(Message::body)
						.doOnNext(latestBinlog -> logger.info("Received binlog from server. Size: {}",
								BinlogUtils.humanReadableByteCountBin(latestBinlog.binlog().length())
						))
						.flatMap(latestBinlog -> this.saveBinlog(latestBinlog.binlog()))
						.doOnSuccess(s -> logger.info("Overwritten binlog from server"))
						.doFirst(() -> logger.info("Asking binlog to server"))
						.thenReturn(update);
			}
		}
		return Mono.just(update);
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function<T> request, Duration timeout,
			boolean executeSync) {
		var req = new ExecuteObject<>(executeSync, request, timeout);
		var deliveryOptions = new DeliveryOptions(this.deliveryOptions)
				// Timeout + 5s (5 seconds extra are used to wait the graceful server-side timeout response)
				.setSendTimeout(timeout.toMillis() + 5000);

		var executionMono = cluster.getEventBus()
				.<TdResultMessage>rxRequest(botAddress + ".execute", req, deliveryOptions)
				.to(RxJava2Adapter::singleToMono)
				.onErrorMap(ex -> ResponseError.newResponseError(request, botAlias, ex))
				.<TdResult<T>>handle((resp, sink) -> {
					if (resp.body() == null) {
						var tdError = new TdError(500, "Response is empty");
						sink.error(ResponseError.newResponseError(request, botAlias, tdError));
					} else {
						sink.next(resp.body().toTdResult());
					}
				})
				.doFirst(() -> logger.trace("Executing request {}", request))
				.doOnSuccess(s -> logger.trace("Executed request {}", request))
				.doOnError(ex -> logger.debug("Failed request {}: {}", req, ex));

		return executionMono
				.transformDeferred(mono -> {
					var crash = this.crash.get();
					if (crash != null) {
						logger.debug("Failed request {} because the TDLib session was already crashed", request);
						return Mono.empty();
					} else {
						return mono;
					}
				})
				.switchIfEmpty(Mono.error(() -> ResponseError.newResponseError(request, botAlias,
						new TdError(500, "The client is closed or the response is empty"))));
	}
}
