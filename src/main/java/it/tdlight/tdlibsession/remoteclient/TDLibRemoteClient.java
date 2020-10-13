package it.tdlight.tdlibsession.remoteclient;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import it.tdlight.common.Init;
import it.tdlight.common.utils.CantLoadLibrary;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.tdlight.utils.MonoUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class TDLibRemoteClient implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(TDLibRemoteClient.class);

	private final SecurityInfo securityInfo;
	private final String masterHostname;
	private final String netInterface;
	private final int port;
	private final Set<String> membersAddresses;
	private final LinkedHashSet<String> botIds;
	private final ReplayProcessor<TdClusterManager> clusterManager = ReplayProcessor.cacheLast();

	public TDLibRemoteClient(SecurityInfo securityInfo, String masterHostname, String netInterface, int port, Set<String> membersAddresses, Set<String> botIds) {
		this.securityInfo = securityInfo;
		this.masterHostname = masterHostname;
		this.netInterface = netInterface;
		this.port = port;
		this.membersAddresses = membersAddresses;
		this.botIds = new LinkedHashSet<>(botIds);

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

		Set<String> botIds = Set.of(args[3].split(","));

		Path keyStorePath = Paths.get(args[4]);
		Path keyStorePasswordPath = Paths.get(args[5]);
		Path trustStorePath = Paths.get(args[6]);
		Path trustStorePasswordPath = Paths.get(args[7]);

		var loggerContext = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		loggerContext.setConfigLocation(TDLibRemoteClient.class.getResource("/tdlib-session-container-log4j2.xml").toURI());

		var securityInfo = new SecurityInfo(keyStorePath, keyStorePasswordPath, trustStorePath, trustStorePasswordPath);

		new TDLibRemoteClient(securityInfo, masterHostname, netInterface, port, membersAddresses, botIds).run(x -> {});
	}

	public void start(Handler<Void> startedEventHandler) throws IllegalStateException {
		run(startedEventHandler);
	}

	public void run(Handler<Void> startedEventHandler) {
		try {
			// Set verbosity level here, before creating the bots
			if (Files.notExists(Paths.get("logs"))) {
				try {
					Files.createDirectory(Paths.get("logs"));
				} catch (FileAlreadyExistsException ignored) {
				}
			}

			logger.info("TDLib remote client is being hosted on" + netInterface + ":" + port + ". Master: " + masterHostname);

			var keyStoreOptions = new JksOptions()
					.setPath(securityInfo.getKeyStorePath().toAbsolutePath().toString())
					.setPassword(securityInfo.getKeyStorePassword());

			var trustStoreOptions = new JksOptions()
					.setPath(securityInfo.getTrustStorePath().toAbsolutePath().toString())
					.setPassword(securityInfo.getTrustStorePassword());

			Mono<TdClusterManager> flux;
			if (!botIds.isEmpty()) {
				flux = TdClusterManager.ofNodes(keyStoreOptions,
						trustStoreOptions,
						false,
						masterHostname,
						netInterface,
						port,
						membersAddresses
				);
			} else {
				flux = Mono.empty();
			}

			flux
					.doOnNext(clusterManager::onNext)
					.doOnTerminate(clusterManager::onComplete)
					.doOnError(clusterManager::onError)
					.flatMapIterable(clusterManager -> botIds
							.stream()
							.map(id -> Map.entry(clusterManager, id))
							.collect(Collectors.toList()))
					.flatMap(entry -> Mono.<String>create(sink -> {
						entry
								.getKey()
								.getVertx()
								.deployVerticle(new AsyncTdMiddleEventBusServer(entry.getKey()),
										new DeploymentOptions().setConfig(new JsonObject()
												.put("botAddress", entry.getValue())
												.put("botAlias", entry.getValue())
												.put("local", false)),
										MonoUtils.toHandler(sink)
								);
					}))
					.doOnError(ex -> {
				logger.error(ex.getLocalizedMessage(), ex);
			}).subscribe(i -> {}, e -> {}, () -> startedEventHandler.handle(null));
		} catch (IOException ex) {
			logger.error("Remote client error", ex);
		}
	}

	@Override
	public void close() {
		clusterManager.blockFirst();
	}
}
