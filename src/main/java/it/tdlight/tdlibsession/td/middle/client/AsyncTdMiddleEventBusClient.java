package it.tdlight.tdlibsession.td.middle.client;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
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
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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

	private final One<BinlogAsyncFile> binlog = Sinks.one();

	private final One<MessageConsumer<TdResultList>> updates = Sinks.one();
	// This will only result in a successful completion, never completes in other ways
	private final Empty<Void> updatesStreamEnd = Sinks.one();
	// This will only result in a crash, never completes in other ways
	private final Empty<Void> crash = Sinks.one();
	// This will only result in a successful completion, never completes in other ways
	private final Empty<Void> pingFail = Sinks.one();
	// This will only result in a successful completion, never completes in other ways.
	// It will be called when UpdateAuthorizationStateClosing is intercepted.
	// If it's completed stop checking if the ping works or not
	private final Empty<Void> authStateClosing = Sinks.one();

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
		return this.binlog.asMono().flatMap(binlog -> BinlogUtils.saveBinlog(binlog, data));
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
		return MonoUtils
				.emitValue(this.binlog, binlog)
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
					return setupUpdatesListener()
							.then(Mono.defer(() -> {
								if (local) {
									return Mono.empty();
								}
								logger.trace("Requesting bots.start-bot");
								return cluster.getEventBus()
										.<byte[]>rxRequest("bots.start-bot", msg).as(MonoUtils::toMono)
										.doOnSuccess(s -> logger.trace("bots.start-bot returned successfully"))
										.subscribeOn(Schedulers.boundedElastic());
							}))
							.then(setupPing());
				});
	}

	private Mono<Void> setupPing() {
		return Mono.<Void>fromCallable(() -> {
			logger.trace("Setting up ping");
			// Disable ping on local servers
			if (!local) {
				Mono
						.defer(() -> {
							logger.trace("Requesting ping...");
							return cluster.getEventBus()
									.<byte[]>rxRequest(botAddress + ".ping", EMPTY, pingDeliveryOptions)
									.as(MonoUtils::toMono);
						})
						.flatMap(msg -> Mono.fromCallable(() -> msg.body()).subscribeOn(Schedulers.boundedElastic()))
						.repeatWhen(l -> l.delayElements(Duration.ofSeconds(10)).takeWhile(x -> true))
						.takeUntilOther(Mono.firstWithSignal(this.updatesStreamEnd.asMono().doOnTerminate(() -> {
							logger.trace("About to kill pinger because updates stream ended");
						}), this.crash.asMono().onErrorResume(ex -> Mono.empty()).doOnTerminate(() -> {
							logger.trace("About to kill pinger because it has seen a crash signal");
						})))
						.doOnNext(s -> logger.trace("PING"))
						.then()
						.onErrorResume(ex -> {
							logger.warn("Ping failed: {}", ex.getMessage());
							return Mono.empty();
						})
						.doOnNext(s -> logger.debug("END PING"))
						.then(MonoUtils.emitEmpty(this.pingFail))
						.subscribeOn(Schedulers.parallel())
						.subscribe();
			}
			logger.trace("Ping setup success");
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<Void> setupUpdatesListener() {
		return Mono
				.fromRunnable(() -> logger.trace("Setting up updates listener..."))
				.then(MonoUtils.<MessageConsumer<TdResultList>>fromBlockingSingle(() -> MessageConsumer
						.newInstance(cluster.getEventBus().<TdResultList>consumer(botAddress + ".updates")
								.setMaxBufferedMessages(5000)
								.getDelegate()
						))
				)
				.flatMap(updateConsumer -> {
					// Return when the registration of all the consumers has been done across the cluster
					return Mono
							.fromRunnable(() -> logger.trace("Emitting updates flux to sink"))
							.then(MonoUtils.emitValue(updates, updateConsumer))
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
				.then(updates.asMono())
				.publishOn(Schedulers.parallel())
				.timeout(Duration.ofSeconds(30))
				.doOnSuccess(s -> logger.trace("Registering updates flux"))
				.flatMapMany(updatesMessageConsumer -> MonoUtils.fromMessageConsumer(Mono
						.empty()
						.doOnSuccess(s -> logger.trace("Sending ready-to-receive"))
						.then(cluster.getEventBus().<byte[]>rxRequest(botAddress + ".ready-to-receive",
								EMPTY,
								deliveryOptionsWithTimeout
						).as(MonoUtils::toMono))
						.doOnSuccess(s -> logger.trace("Sent ready-to-receive, received reply"))
						.doOnSuccess(s -> logger.trace("About to read updates flux"))
						.then(), updatesMessageConsumer)
				)
				.takeUntilOther(Flux
						.merge(
								crash.asMono()
										.onErrorResume(ex -> {
											logger.error("TDLib crashed", ex);
											return Mono.empty();
										}),
								pingFail.asMono()
										.then(Mono.fromCallable(() -> {
											var ex = new ConnectException("Server did not respond to ping");
											ex.setStackTrace(new StackTraceElement[0]);
											throw ex;
										}).onErrorResume(ex -> MonoUtils.emitError(crash, ex)))
										.takeUntilOther(Mono
												.firstWithSignal(crash.asMono(), authStateClosing.asMono())
												.onErrorResume(e -> Mono.empty())
										)
						)
						.doOnTerminate(() -> logger.trace("TakeUntilOther has been trigghered, the receive() flux will end"))
				)
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
				.doOnError(error -> crash.tryEmitError(error))
				.onErrorResume(ex -> {
					logger.trace("Absorbing the error, the error has been published using the crash sink", ex);
					return Mono.empty();
				})

				.doOnTerminate(updatesStreamEnd::tryEmitEmpty);
	}

	private Mono<TdApi.Object> interceptUpdate(Object update) {
		logger.trace("Received update {}", update.getClass().getSimpleName());
		switch (update.getConstructor()) {
			case TdApi.UpdateAuthorizationState.CONSTRUCTOR:
				var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
				switch (updateAuthorizationState.authorizationState.getConstructor()) {
					case TdApi.AuthorizationStateClosing.CONSTRUCTOR:
						authStateClosing.tryEmitEmpty();
						break;
					case TdApi.AuthorizationStateClosed.CONSTRUCTOR:
						return Mono.fromRunnable(() -> logger.info("Received AuthorizationStateClosed from tdlib"))
								.then(cluster.getEventBus().<EndSessionMessage>rxRequest(this.botAddress + ".read-binlog", EMPTY).as(MonoUtils::toMono))
								.flatMap(latestBinlogMsg -> Mono.fromCallable(() -> latestBinlogMsg.body()).subscribeOn(Schedulers.parallel()))
								.doOnNext(latestBinlog -> logger.info("Received binlog from server. Size: " + BinlogUtils.humanReadableByteCountBin(latestBinlog.binlog().length())))
								.flatMap(latestBinlog -> this.saveBinlog(latestBinlog.binlog()))
								.doOnSuccess(s -> logger.info("Overwritten binlog from server"))
								.thenReturn(update);
				}
				break;
		}
		return Mono.just(update);
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {
		var req = new ExecuteObject(executeDirectly, request);
		return Mono
				.firstWithSignal(
						MonoUtils
								.castVoid(crash
										.asMono()
										.doOnSuccess(s -> logger
												.debug("Failed request {} because the TDLib session was already crashed", request))
								),
						Mono
								.fromRunnable(() -> logger.trace("Executing request {}", request))
								.then(cluster.getEventBus().<TdResultMessage>rxRequest(botAddress + ".execute", req, deliveryOptions).as(MonoUtils::toMono))
								.onErrorMap(ex -> ResponseError.newResponseError(request, botAlias, ex))
								.<TdResult<T>>handle((resp, sink) -> {
									if (resp.body() == null) {
										sink.error(ResponseError.newResponseError(request, botAlias, new TdError(500, "Response is empty")));
									} else {
										sink.next(resp.body().toTdResult());
									}
								})
								.doOnSuccess(s -> logger.trace("Executed request {}", request))
								.doOnError(ex -> logger.debug("Failed request {}: {}", req, ex))
		)
				.switchIfEmpty(Mono.defer(() -> Mono.fromCallable(() -> {
					throw ResponseError.newResponseError(request, botAlias, new TdError(500, "Client is closed or response is empty"));
				})));
	}
}
