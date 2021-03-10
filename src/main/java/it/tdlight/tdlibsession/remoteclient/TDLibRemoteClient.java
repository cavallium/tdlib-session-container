package it.tdlight.tdlibsession.remoteclient;

import com.akaita.java.rxjava2debug.RxJava2Debug;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import it.tdlight.common.Init;
import it.tdlight.common.utils.CantLoadLibrary;
import it.tdlight.tdlibsession.td.middle.StartSessionMessage;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer;
import it.tdlight.utils.BinlogUtils;
import it.tdlight.utils.MonoUtils;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.tools.agent.ReactorDebugAgent;
import reactor.util.function.Tuple2;

public class TDLibRemoteClient implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(TDLibRemoteClient.class);

	@Nullable
	private final SecurityInfo securityInfo;
	private final String masterHostname;
	private final String netInterface;
	private final int port;
	private final Set<String> membersAddresses;
	private final One<TdClusterManager> clusterManager = Sinks.one();
	/**
	 * Statistic about active deployments count
	 */
	private final AtomicInteger statsActiveDeployments = new AtomicInteger();

	public static boolean runningFromIntelliJ() {
		return System.getProperty("java.class.path").contains("idea_rt.jar")
				|| System.getProperty("idea.test.cyclic.buffer.size") != null;
	}

	public TDLibRemoteClient(@Nullable SecurityInfo securityInfo,
			String masterHostname,
			String netInterface,
			int port,
			Set<String> membersAddresses,
			boolean enableAsyncStacktraces,
			boolean enableFullAsyncStacktraces) {
		this.securityInfo = securityInfo;
		this.masterHostname = masterHostname;
		this.netInterface = netInterface;
		this.port = port;
		this.membersAddresses = membersAddresses;

		if (enableAsyncStacktraces && !runningFromIntelliJ()) {
			ReactorDebugAgent.init();
		}
		if (enableAsyncStacktraces && enableFullAsyncStacktraces) {
			RxJava2Debug.enableRxJava2AssemblyTracking(new String[]{"it.tdlight.utils", "it.tdlight.tdlibsession"});
		}

		try {
			Init.start();
		} catch (CantLoadLibrary ex) {
			throw new RuntimeException(ex);
		}
	}

	public static void main(String[] args) throws URISyntaxException {
		if (args.length < 1) {
			return;
		}

		String masterHostname = args[0];

		String[] interfaceAndPort = args[1].split(":", 2);

		String netInterface = interfaceAndPort[0];

		int port = Integer.parseInt(interfaceAndPort[1]);

		Set<String> membersAddresses = Set.of(args[2].split(","));

		Path keyStorePath = Paths.get(args[3]);
		Path keyStorePasswordPath = Paths.get(args[4]);
		Path trustStorePath = Paths.get(args[5]);
		Path trustStorePasswordPath = Paths.get(args[6]);
		boolean enableAsyncStacktraces = Boolean.parseBoolean(args[7]);
		boolean enableFullAsyncStacktraces = Boolean.parseBoolean(args[8]);

		var loggerContext = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		loggerContext.setConfigLocation(TDLibRemoteClient.class.getResource("/tdlib-session-container-log4j2.xml").toURI());

		var securityInfo = new SecurityInfo(keyStorePath, keyStorePasswordPath, trustStorePath, trustStorePasswordPath);

		var client = new TDLibRemoteClient(securityInfo,
				masterHostname,
				netInterface,
				port,
				membersAddresses,
				enableAsyncStacktraces,
				enableFullAsyncStacktraces
		);

		client
				.start()
				.block();

		// Close vert.x on shutdown
		var vertx = client.clusterManager.asMono().block().getVertx();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> MonoUtils.toMono(vertx.rxClose()).blockOptional()));
	}

	public Mono<Void> start() {
		var ksp = securityInfo == null ? null : securityInfo.getKeyStorePassword(false);
		var keyStoreOptions = securityInfo == null || ksp == null ? null : new JksOptions()
				.setPath(securityInfo.getKeyStorePath().toAbsolutePath().toString())
				.setPassword(ksp);

		var tsp = securityInfo == null ? null : securityInfo.getTrustStorePassword(false);
		var trustStoreOptions = securityInfo == null || tsp == null ? null : new JksOptions()
				.setPath(securityInfo.getTrustStorePath().toAbsolutePath().toString())
				.setPassword(tsp);

		return MonoUtils
				.fromBlockingEmpty(() -> {
					// Set verbosity level here, before creating the bots
					if (Files.notExists(Paths.get("logs"))) {
						try {
							Files.createDirectory(Paths.get("logs"));
						} catch (FileAlreadyExistsException ignored) {
						}
					}

					logger.info(
							"TDLib remote client is being hosted on" + netInterface + ":" + port + ". Master: " + masterHostname);
					logger.info(
							"TDLib remote client SSL enabled: " + (keyStoreOptions != null && trustStoreOptions != null));
				})
				.then(TdClusterManager.ofNodes(keyStoreOptions,
						trustStoreOptions,
						false,
						masterHostname,
						netInterface,
						port,
						membersAddresses
				))
				.doOnNext(clusterManager::tryEmitValue)
				.doOnError(clusterManager::tryEmitError)
				.doOnSuccess(s -> {
					if (s == null) {
						clusterManager.tryEmitEmpty();
					}
				})
				.single()
				.flatMap(clusterManager -> {
					MessageConsumer<StartSessionMessage> startBotConsumer = clusterManager.getEventBus().consumer("bots.start-bot");
					return MonoUtils
							.fromReplyableResolvedMessageConsumer(startBotConsumer)
							.flatMap(tuple -> this.listenForStartBotsCommand(clusterManager, tuple.getT1(), tuple.getT2()));
				})
				.then();
	}

	private Mono<Void> listenForStartBotsCommand(TdClusterManager clusterManager,
			Mono<Void> completion,
			Flux<Tuple2<Message<?>, StartSessionMessage>> messages) {
		return MonoUtils
				.fromBlockingEmpty(() -> messages
						.flatMapSequential(msg -> {
							StartSessionMessage req = msg.getT2();
							DeploymentOptions deploymentOptions = clusterManager
									.newDeploymentOpts()
									.setConfig(new JsonObject()
											.put("botId", req.id())
											.put("botAlias", req.alias())
											.put("local", false)
											.put("implementationDetails", req.implementationDetails()));
							var verticle = new AsyncTdMiddleEventBusServer();

							// Binlog path
							var sessPath = getSessionDirectory(req.id());
							var mediaPath = getMediaDirectory(req.id());
							var blPath = getSessionBinlogDirectory(req.id());

							return BinlogUtils
									.chooseBinlog(clusterManager.getVertx().fileSystem(), blPath, req.binlog(), req.binlogDate())
									.then(BinlogUtils.cleanSessionPath(clusterManager.getVertx().fileSystem(), blPath, sessPath, mediaPath))
									.then(clusterManager.getVertx().rxDeployVerticle(verticle, deploymentOptions).as(MonoUtils::toMono))
									.then(MonoUtils.fromBlockingEmpty(() -> msg.getT1().reply(new byte[0])))
									.onErrorResume(ex -> {
										msg.getT1().fail(500, "Failed to deploy bot verticle: " + ex.getMessage());
										logger.error("Failed to deploy bot verticle", ex);
										return Mono.empty();
									});
						})
						.subscribeOn(Schedulers.parallel())
						.subscribe(
								v -> {},
								ex -> logger.error("Bots starter activity crashed. From now on, no new bots can be started anymore", ex)
						)
				)
				.then(completion);
	}

	public static Path getSessionDirectory(long botId) {
		return Paths.get(".sessions-cache").resolve("id" + botId);
	}

	public static Path getMediaDirectory(long botId) {
		return Paths.get(".cache").resolve("media").resolve("id" + botId);
	}

	public static Path getSessionBinlogDirectory(long botId) {
		return getSessionDirectory(botId).resolve("td.binlog");
	}

	@Override
	public void close() {
		this.clusterManager
				.asMono()
				.blockOptional()
				.map(TdClusterManager::getVertx)
				.ifPresent(v -> v.rxClose().blockingAwait());
	}
}
