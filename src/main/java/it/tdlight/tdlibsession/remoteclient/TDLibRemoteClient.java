package it.tdlight.tdlibsession.remoteclient;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;

public class TDLibRemoteClient implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(TDLibRemoteClient.class);

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

	public TDLibRemoteClient(SecurityInfo securityInfo, String masterHostname, String netInterface, int port, Set<String> membersAddresses) {
		this.securityInfo = securityInfo;
		this.masterHostname = masterHostname;
		this.netInterface = netInterface;
		this.port = port;
		this.membersAddresses = membersAddresses;

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

		var loggerContext = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		loggerContext.setConfigLocation(TDLibRemoteClient.class.getResource("/tdlib-session-container-log4j2.xml").toURI());

		var securityInfo = new SecurityInfo(keyStorePath, keyStorePasswordPath, trustStorePath, trustStorePasswordPath);

		new TDLibRemoteClient(securityInfo, masterHostname, netInterface, port, membersAddresses)
				.start()
				.block();
	}

	public Mono<Void> start() {
		var keyStoreOptions = new JksOptions()
				.setPath(securityInfo.getKeyStorePath().toAbsolutePath().toString())
				.setPassword(securityInfo.getKeyStorePassword());

		var trustStoreOptions = new JksOptions()
				.setPath(securityInfo.getTrustStorePath().toAbsolutePath().toString())
				.setPassword(securityInfo.getTrustStorePassword());

		return MonoUtils
				.fromBlockingSingle(() -> {
					// Set verbosity level here, before creating the bots
					if (Files.notExists(Paths.get("logs"))) {
						try {
							Files.createDirectory(Paths.get("logs"));
						} catch (FileAlreadyExistsException ignored) {
						}
					}

					logger.info(
							"TDLib remote client is being hosted on" + netInterface + ":" + port + ". Master: " + masterHostname);
					return null;
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
				.flatMap(clusterManager -> {
					MessageConsumer<StartSessionMessage> startBotConsumer = clusterManager.getEventBus().consumer("bots.start-bot");
					startBotConsumer.handler(msg -> {

						StartSessionMessage req = msg.body();
						DeploymentOptions deploymentOptions = clusterManager
								.newDeploymentOpts()
								.setConfig(new JsonObject()
										.put("botId", req.id())
										.put("botAlias", req.alias())
										.put("local", false));
						var verticle = new AsyncTdMiddleEventBusServer();

						// Binlog path
						var blPath = Paths.get(".sessions-cache").resolve("id" + req.id()).resolve("td.binlog");

						BinlogUtils
								.chooseBinlog(clusterManager.getVertx().fileSystem(), blPath, req.binlog(), req.binlogDate())
								.then(clusterManager.getVertx().rxDeployVerticle(verticle, deploymentOptions).as(MonoUtils::toMono))
								.subscribeOn(Schedulers.single())
								.subscribe(
										v -> {},
										ex -> {
											logger.error("Failed to deploy bot verticle", ex);
											msg.fail(500, "Failed to deploy bot verticle: " + ex.getMessage());
										},
										() -> msg.reply(new byte[0])
								);
					});
					return Mono.empty();
				})
				.then();
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
