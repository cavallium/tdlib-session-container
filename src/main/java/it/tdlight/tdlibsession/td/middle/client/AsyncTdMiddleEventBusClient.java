package it.tdlight.tdlibsession.td.middle.client;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.Message;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.TdResultMessage;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.ExecuteObject;
import it.tdlight.tdlibsession.td.middle.StartSessionMessage;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.TdResultList;
import it.tdlight.utils.BinlogAsyncFile;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import it.tdlight.utils.MonoUtils.SinkRWStream;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private final One<BinlogAsyncFile> binlog = Sinks.one();

	SinkRWStream<Message<TdResultList>> updates = MonoUtils.unicastBackpressureStream(1000);
	// This will only result in a successful completion, never completes in other ways
	private final Empty<Void> updatesStreamEnd = Sinks.one();
	// This will only result in a crash, never completes in other ways
	private final Empty<Void> crash = Sinks.one();

	private int botId;
	private String botAddress;
	private String botAlias;
	private boolean local;

	public AsyncTdMiddleEventBusClient(TdClusterManager clusterManager) {
		this.logger = LoggerFactory.getLogger(AsyncTdMiddleEventBusClient.class);
		this.cluster = clusterManager;
		this.deliveryOptions = cluster.newDeliveryOpts().setLocalOnly(local);
		this.deliveryOptionsWithTimeout = cluster.newDeliveryOpts().setLocalOnly(local).setSendTimeout(30000);
	}

	public static Mono<AsyncTdMiddle> getAndDeployInstance(TdClusterManager clusterManager,
			int botId,
			String botAlias,
			boolean local,
			Path binlogsArchiveDirectory) {
		var instance = new AsyncTdMiddleEventBusClient(clusterManager);
		return retrieveBinlog(clusterManager.getVertx(), binlogsArchiveDirectory, botId)
				.single()
				.flatMap(binlog -> instance
						.start(botId, botAlias, local, binlog)
						.thenReturn(instance)
				);
	}

	/**
	 *
	 * @return optional result
	 */
	public static Mono<BinlogAsyncFile> retrieveBinlog(Vertx vertx, Path binlogsArchiveDirectory, int botId) {
		return BinlogUtils.retrieveBinlog(vertx.fileSystem(), binlogsArchiveDirectory.resolve(botId + ".binlog"));
	}

	private Mono<Void> saveBinlog(byte[] data) {
		return this.binlog.asMono().flatMap(binlog -> BinlogUtils.saveBinlog(binlog, data));
	}

	public Mono<Void> start(int botId, String botAlias, boolean local, BinlogAsyncFile binlog) {
		this.botId = botId;
		this.botAlias = botAlias;
		this.botAddress = "bots.bot." + this.botId;
		this.local = local;
		this.logger = LoggerFactory.getLogger(this.botId + " " + botAlias);
		return MonoUtils
				.emitValue(this.binlog, binlog)
				.then(binlog.getLastModifiedTime())
				.zipWith(binlog.readFullyBytes())
				.single()
				.flatMap(tuple -> {
					var binlogLastModifiedTime = tuple.getT1();
					var binlogData = tuple.getT2();

					var msg = new StartSessionMessage(this.botId, this.botAlias, binlogData, binlogLastModifiedTime);
					return setupUpdatesListener()
							.then(cluster.getEventBus().<byte[]>rxRequest("bots.start-bot", msg).as(MonoUtils::toMono))
							.then();
				});
	}

	@SuppressWarnings("CallingSubscribeInNonBlockingScope")
	private Mono<Void> setupUpdatesListener() {
		var updateConsumer = cluster.getEventBus().<TdResultList>consumer(botAddress + ".update");
		updateConsumer.endHandler(h -> {
			logger.error("<<<<<<<<<<<<<<<<EndHandler?>>>>>>>>>>>>>");
		});

		// Here the updates will be piped from the server to the client
		updateConsumer
				.rxPipeTo(updates.writeAsStream()).as(MonoUtils::toMono)
				.subscribeOn(Schedulers.single())
				.subscribe();

		// Return when the registration of all the consumers has been done across the cluster
		return updateConsumer.rxCompletionHandler().as(MonoUtils::toMono);
	}

	@Override
	public Flux<TdApi.Object> receive() {
		// Here the updates will be received
		return updates
				.readAsFlux()
				.subscribeOn(Schedulers.single())
				.flatMap(updates -> Flux.fromIterable(updates.body().getValues()))
				.flatMap(update -> Mono.fromCallable(update::orElseThrow))
				.doOnError(crash::tryEmitError)
				.doOnTerminate(updatesStreamEnd::tryEmitEmpty);
	}

	@Override
	public <T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {
		var req = new ExecuteObject(executeDirectly, request);
		return Mono
				.firstWithSignal(
						MonoUtils.castVoid(crash.asMono()),
						cluster.getEventBus()
								.<TdResultMessage>rxRequest(botAddress + ".execute", req, deliveryOptionsWithTimeout).as(MonoUtils::toMono)
								.onErrorMap(ex -> ResponseError.newResponseError(request, botAlias, ex))
								.<TdResult<T>>flatMap(resp -> Mono.fromCallable(() -> {
									if (resp.body() == null) {
										throw ResponseError.newResponseError(request, botAlias, new TdError(500, "Response is empty"));
									} else {
										return resp.body().toTdResult();
									}
								}))
		)
				.switchIfEmpty(Mono.fromCallable(() -> {
					throw ResponseError.newResponseError(request, botAlias, new TdError(500, "Client is closed or response is empty"));
				}));
	}
}
