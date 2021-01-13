package it.tdlight.tdlibsession.remoteclient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.net.JksOptions;
import io.vertx.core.shareddata.AsyncMap;
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
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TDLibRemoteClient implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(TDLibRemoteClient.class);

	private final SecurityInfo securityInfo;
	private final String masterHostname;
	private final String netInterface;
	private final int port;
	private final Set<String> membersAddresses;
	private final Many<TdClusterManager> clusterManager = Sinks.many().replay().latest();
	private final Scheduler deploymentScheduler = Schedulers.newSingle("TDLib", false);

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

		new TDLibRemoteClient(securityInfo, masterHostname, netInterface, port, membersAddresses).run(x -> {});
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

			var botAddresses = new RemoteClientBotAddresses(Paths.get("remote_client_bot_addresses.txt"));
			botAddresses.values().forEach(botAddress -> logger.info("Bot address is registered on this cluster:" + botAddress));

			var keyStoreOptions = new JksOptions()
					.setPath(securityInfo.getKeyStorePath().toAbsolutePath().toString())
					.setPassword(securityInfo.getKeyStorePassword());

			var trustStoreOptions = new JksOptions()
					.setPath(securityInfo.getTrustStorePath().toAbsolutePath().toString())
					.setPassword(securityInfo.getTrustStorePassword());

			TdClusterManager.ofNodes(keyStoreOptions,
						trustStoreOptions,
						false,
						masterHostname,
						netInterface,
						port,
						membersAddresses
				)
					.doOnNext(clusterManager::tryEmitNext)
					.doOnTerminate(clusterManager::tryEmitComplete)
					.doOnError(clusterManager::tryEmitError)
					.flatMapMany(clusterManager -> {
						return Flux.create(sink -> {
							clusterManager.getSharedData().getClusterWideMap("deployableBotAddresses", mapResult -> {
								if (mapResult.succeeded()) {
									var deployableBotAddresses = mapResult.result();

									clusterManager.getSharedData().getLockWithTimeout("deployment", 15000, lockAcquisitionResult -> {
										if (lockAcquisitionResult.succeeded()) {
											var deploymentLock = lockAcquisitionResult.result();
											putAllAsync(deployableBotAddresses, botAddresses.values(), (AsyncResult<Void> putAllResult) -> {
												if (putAllResult.succeeded()) {
													listenForDeployRequests(botAddresses,
															clusterManager,
															sink,
															deployableBotAddresses
													);
												} else {
													logger.error("Can't update shared map", putAllResult.cause());
													sink.error(putAllResult.cause());
												}
												deploymentLock.release();
											});
										} else {
											logger.error("Can't obtain deployment lock", lockAcquisitionResult.cause());
											sink.error(lockAcquisitionResult.cause());
										}
									});
								} else {
									logger.error("Can't get shared map", mapResult.cause());
									sink.error(mapResult.cause());
								}
							});
						});
					})
					.doOnError(ex -> {
				logger.error(ex.getLocalizedMessage(), ex);
			}).subscribeOn(deploymentScheduler).subscribe(i -> {}, e -> {
				logger.error("Remote client error", e);
			}, () -> startedEventHandler.handle(null));
		} catch (IOException ex) {
			logger.error("Remote client error", ex);
		}
	}

	private void listenForDeployRequests(RemoteClientBotAddresses botAddresses,
			TdClusterManager clusterManager,
			reactor.core.publisher.FluxSink<Object> sink,
			AsyncMap<Object, Object> deployableBotAddresses) {
		clusterManager.getEventBus().consumer("tdlib.remoteclient.clients.deploy", (Message<String> msg) -> {

			clusterManager.getSharedData().getLockWithTimeout("deployment", 15000, lockAcquisitionResult -> {
				if (lockAcquisitionResult.succeeded()) {
					var deploymentLock = lockAcquisitionResult.result();
					var botAddress = msg.body();
					if (botAddresses.has(botAddress)) {
						deployBot(clusterManager, botAddress, deploymentResult -> {
							if (deploymentResult.failed()) {
								msg.fail(500,
										"Failed to deploy existing bot \"" + botAddress + "\": " + deploymentResult
												.cause()
												.getLocalizedMessage()
								);
								sink.error(deploymentResult.cause());
							} else {
								sink.next(botAddress);
							}
							deploymentLock.release();
						});
					} else {
						deployableBotAddresses.putIfAbsent(botAddress, netInterface, putResult -> {
							if (putResult.succeeded()) {
								if (putResult.result() == null) {
									logger.info("Deploying new bot at address \"" + botAddress + "\"");
									try {
										botAddresses.putAddress(botAddress);
									} catch (IOException e) {
										logger.error("Can't save bot address \"" + botAddress + "\" to addresses file", e);
									}
									deployBot(clusterManager, botAddress, deploymentResult -> {
										if (deploymentResult.failed()) {
											msg.fail(500,
													"Failed to deploy new bot \"" + botAddress + "\": " + deploymentResult
															.cause()
															.getLocalizedMessage()
											);
											sink.error(deploymentResult.cause());
										} else {
											sink.next(botAddress);
										}
										deploymentLock.release();
									});
								} else {
									// Ignore this bot because it's present on another cluster
									deploymentLock.release();
								}
							} else {
								logger.error("Can't update shared map", putResult.cause());
								sink.error(putResult.cause());
								deploymentLock.release();
							}
						});
					}
				} else {
					logger.error("Can't obtain deployment lock", lockAcquisitionResult.cause());
					sink.error(lockAcquisitionResult.cause());
				}
			});
		});
	}

	private void deployBot(TdClusterManager clusterManager, String botAddress, Handler<AsyncResult<String>> deploymentHandler) {
		AsyncTdMiddleEventBusServer verticle = new AsyncTdMiddleEventBusServer(clusterManager);
		verticle.onBeforeStop(handler -> {
			clusterManager.getSharedData().getLockWithTimeout("deployment", 30000, lockAcquisitionResult -> {
				if (lockAcquisitionResult.succeeded()) {
					var deploymentLock = lockAcquisitionResult.result();
					verticle.onAfterStop(handler2 -> {
						deploymentLock.release();
						handler2.complete();
					});
					clusterManager.getSharedData().getClusterWideMap("runningBotAddresses", (AsyncResult<AsyncMap<String, String>> mapResult) -> {
						if (mapResult.succeeded()) {
							var runningBotAddresses = mapResult.result();
							runningBotAddresses.removeIfPresent(botAddress, netInterface, putResult -> {
								if (putResult.succeeded()) {
									if (putResult.result() != null) {
										handler.complete();
									} else {
										handler.fail("Can't destroy bot with address \"" + botAddress + "\" because it has been already destroyed");
									}
								} else {
									handler.fail(putResult.cause());
								}
							});
						} else {
							handler.fail(mapResult.cause());
						}
					});
				} else {
					handler.fail(lockAcquisitionResult.cause());
				}
			});
		});
		verticle.start(botAddress, botAddress, false).doOnError(error -> {
			logger.error("Can't deploy bot \"" + botAddress + "\"", error);
		}).subscribeOn(deploymentScheduler).subscribe(v -> {}, err -> {
			deploymentHandler.handle(Future.failedFuture(err));
		}, () -> {
			deploymentHandler.handle(Future.succeededFuture());
		});
	}

	private void putAllAsync(AsyncMap<Object, Object> sharedMap,
			Set<String> valuesToAdd,
			Handler<AsyncResult<Void>> resultHandler) {
		if (valuesToAdd.isEmpty()) {
			resultHandler.handle(Future.succeededFuture());
		} else {
			var valueToAdd = valuesToAdd.stream().findFirst().get();
			valuesToAdd.remove(valueToAdd);
			sharedMap.putIfAbsent(valueToAdd, netInterface, result -> {
				if (result.succeeded()) {
					if (result.result() == null || result.result().equals(netInterface)) {
						putAllAsync(sharedMap, valuesToAdd, resultHandler);
					} else {
						resultHandler.handle(Future.failedFuture(new UnsupportedOperationException("Key already present! Key: \"" + valueToAdd + "\", Value: \"" + result.result() + "\"")));
					}
				} else {
					resultHandler.handle(Future.failedFuture(result.cause()));
				}
			});
		}
	}

	@Override
	public void close() {
		clusterManager.asFlux().blockFirst();
		deploymentScheduler.dispose();
	}
}
