package it.tdlight.tdlibsession.td.middle;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import io.vertx.reactivex.core.shareddata.SharedData;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import it.tdlight.common.ConstructorDetector;
import it.tdlight.jni.TdApi;
import it.tdlight.utils.MonoUtils;
import java.nio.channels.AlreadyBoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class TdClusterManager {

	private static final AtomicBoolean definedMasterCluster = new AtomicBoolean(false);
	private static final AtomicBoolean definedNodesCluster = new AtomicBoolean(false);
	private final ClusterManager mgr;
	private final VertxOptions vertxOptions;
	private final Vertx vertx;
	private final boolean isMaster;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public TdClusterManager(ClusterManager mgr, VertxOptions vertxOptions, Vertx vertx, boolean isMaster) {
		this.isMaster = isMaster;
		this.mgr = mgr;
		this.vertxOptions = vertxOptions;
		this.vertx = vertx;

		if (vertx != null && vertx.eventBus() != null) {
			vertx
					.eventBus()
					.getDelegate()
					.registerDefaultCodec(TdResultList.class, new TdResultListMessageCodec())
					.registerDefaultCodec(ExecuteObject.class, new TdExecuteObjectMessageCodec())
					.registerDefaultCodec(TdResultMessage.class, new TdResultMessageCodec())
					.registerDefaultCodec(StartSessionMessage.class, new StartSessionMessageCodec())
					.registerDefaultCodec(EndSessionMessage.class, new EndSessionMessageCodec());
			for (Class<?> declaredClass : TdApi.class.getDeclaredClasses()) {
				if (declaredClass.isAssignableFrom(declaredClass)) {
					vertx.eventBus().getDelegate().registerDefaultCodec(declaredClass, new TdMessageCodec(declaredClass));
				}
			}
		}
	}

	public static Mono<TdClusterManager> ofMaster(@Nullable JksOptions keyStoreOptions,
			@Nullable JksOptions trustStoreOptions,
			boolean onlyLocal,
			String masterHostname,
			String netInterface,
			int port,
			Set<String> nodesAddresses) {
		if (definedMasterCluster.compareAndSet(false, true)) {
			var vertxOptions = new VertxOptions();
			netInterface = onlyLocal ? "127.0.0.1" : netInterface;
			Config cfg;
			if (!onlyLocal) {
				cfg = new Config();
				cfg.setInstanceName("Master");
			} else {
				cfg = null;
			}
			return of(cfg,
					vertxOptions,
					keyStoreOptions, trustStoreOptions, masterHostname, netInterface, port, nodesAddresses, true);
		} else {
			return Mono.error(new AlreadyBoundException());
		}
	}

	public static Mono<TdClusterManager> ofNodes(@Nullable JksOptions keyStoreOptions,
			@Nullable JksOptions trustStoreOptions,
			boolean onlyLocal,
			String masterHostname,
			String netInterface,
			int port,
			Set<String> nodesAddresses) {
		return Mono.defer(() -> {
			if (definedNodesCluster.compareAndSet(false, true)) {
				var vertxOptions = new VertxOptions();
				var netInterfaceF = onlyLocal ? "127.0.0.1" : netInterface;
				Config cfg;
				if (!onlyLocal) {
					cfg = new Config();
					cfg.setInstanceName("Node-" + new Random().nextLong());
				} else {
					cfg = null;
				}
				return of(cfg, vertxOptions, keyStoreOptions, trustStoreOptions, masterHostname, netInterfaceF, port,
						nodesAddresses, false);
			} else {
				return Mono.error(new AlreadyBoundException());
			}
		});
	}

	public static Mono<TdClusterManager> of(@Nullable Config cfg,
			VertxOptions vertxOptions,
			@Nullable JksOptions keyStoreOptions,
			@Nullable JksOptions trustStoreOptions,
			String masterHostname,
			String netInterface,
			int port,
			Set<String> nodesAddresses,
			boolean isMaster) {
		ClusterManager mgr;
		if (cfg != null) {
			cfg.getNetworkConfig().setPortCount(1);
			cfg.getNetworkConfig().setPort(port);
			cfg.getNetworkConfig().setPortAutoIncrement(false);
			cfg.getPartitionGroupConfig().setEnabled(false);
			cfg.addMapConfig(new MapConfig().setName("__vertx.haInfo").setBackupCount(1));
			cfg.addMapConfig(new MapConfig().setName("__vertx.nodeInfo").setBackupCount(1));
			cfg
					.getCPSubsystemConfig()
					.setCPMemberCount(0)
					.setSemaphoreConfigs(Map.of("__vertx.*", new SemaphoreConfig().setInitialPermits(1).setJDKCompatible(false)));
			cfg.addMultiMapConfig(new MultiMapConfig().setName("__vertx.subs").setBackupCount(1).setValueCollectionType("SET"));
			cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
			cfg.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);
			cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
			var addresses = new ArrayList<>(nodesAddresses);
			cfg.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(addresses);
			cfg.getNetworkConfig().getInterfaces().clear();
			cfg.getNetworkConfig().getInterfaces().setInterfaces(Collections.singleton(netInterface)).setEnabled(true);
			cfg.getNetworkConfig().setOutboundPorts(Collections.singleton(0));

			cfg.setProperty("hazelcast.logging.type", "slf4j");
			cfg.setProperty("hazelcast.wait.seconds.before.join", "0");
			cfg.setProperty("hazelcast.tcp.join.port.try.count", "5");
			cfg.setProperty("hazelcast.socket.bind.any", "false");
			cfg.setProperty("hazelcast.health.monitoring.level", "OFF");
			cfg.setClusterName("tdlib-session-container");
			mgr = new HazelcastClusterManager(cfg);
			vertxOptions.setClusterManager(mgr);
			vertxOptions.getEventBusOptions().setConnectTimeout(120000);
			//vertxOptions.getEventBusOptions().setIdleTimeout(60);
			//vertxOptions.getEventBusOptions().setSsl(false);

			vertxOptions.getEventBusOptions().setSslHandshakeTimeout(120000).setSslHandshakeTimeoutUnit(TimeUnit.MILLISECONDS);
			if (keyStoreOptions != null && trustStoreOptions != null) {
				vertxOptions.getEventBusOptions().setKeyStoreOptions(keyStoreOptions);
				vertxOptions.getEventBusOptions().setTrustStoreOptions(trustStoreOptions);
				vertxOptions
						.getEventBusOptions()
						.setUseAlpn(true)
						.setSsl(true)
						.setEnabledSecureTransportProtocols(Set.of("TLSv1.3", "TLSv1.2"));
			} else {
				vertxOptions
						.getEventBusOptions()
						.setSsl(false);
			}
			vertxOptions.getEventBusOptions().setHost(masterHostname);
			vertxOptions.getEventBusOptions().setPort(port + 1);
			vertxOptions.getEventBusOptions().setClientAuth(ClientAuth.REQUIRED);
		} else {
			mgr = null;
			vertxOptions.setClusterManager(null);
		}

		vertxOptions.setPreferNativeTransport(true);
		vertxOptions.setMetricsOptions(new MetricsOptions().setEnabled(false));
		// check for blocked threads every 5s
		vertxOptions.setBlockedThreadCheckInterval(5);
		vertxOptions.setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS);
		// warn if an event loop thread handler took more than 10s to execute
		vertxOptions.setMaxEventLoopExecuteTime(10);
		vertxOptions.setMaxEventLoopExecuteTimeUnit(TimeUnit.SECONDS);
		// warn if an worker thread handler took more than 10s to execute
		vertxOptions.setMaxWorkerExecuteTime(10);
		vertxOptions.setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS);
		// log the stack trace if an event loop or worker handler took more than 20s to execute
		vertxOptions.setWarningExceptionTime(100);
		vertxOptions.setWarningExceptionTimeUnit(TimeUnit.MILLISECONDS);

		return Mono
				.defer(() -> {
					if (mgr != null) {
						return Vertx.rxClusteredVertx(vertxOptions).as(MonoUtils::toMono).subscribeOn(Schedulers.boundedElastic());
					} else {
						return Mono.just(Vertx.vertx(vertxOptions));
					}
				})
				.flatMap(vertx -> Mono
						.fromCallable(() -> new TdClusterManager(mgr, vertxOptions, vertx, isMaster))
						.subscribeOn(Schedulers.boundedElastic())
				);
	}

	public Vertx getVertx() {
		return vertx;
	}

	public EventBus getEventBus() {
		return vertx.eventBus();
	}

	public VertxOptions getVertxOptions() {
		return vertxOptions;
	}

	public DeliveryOptions newDeliveryOpts() {
		return new DeliveryOptions().setSendTimeout(120000);
	}

	/**
	 *
	 * @param messageCodec
	 * @param <T>
	 * @return true if registered, false if already registered
	 */
	public <T> boolean registerCodec(MessageCodec<T, T> messageCodec) {
		try {
			vertx.eventBus().registerCodec(messageCodec);
			return true;
		} catch (IllegalStateException ex) {
			if (ex.getMessage().startsWith("Already a default codec registered for class")) {
				return false;
			}
			if (ex.getMessage().startsWith("Already a codec registered with name")) {
				return false;
			}
			throw ex;
		}
	}

	/**
	 * Create a message consumer against the specified address.
	 * <p>
	 * The returned consumer is not yet registered
	 * at the address, registration will be effective when {@link MessageConsumer#handler(io.vertx.core.Handler)}
	 * is called.
	 *
	 * @param address  the address that it will register it at
	 * @param localOnly if you want to receive only local messages
	 * @return the event bus message consumer
	 */
	public <T> MessageConsumer<T> consumer(String address, boolean localOnly) {
		if (localOnly) {
			return vertx.eventBus().localConsumer(address);
		} else {
			return vertx.eventBus().consumer(address);
		}
	}

	/**
	 * Create a consumer and register it against the specified address.
	 *
	 * @param address  the address that will register it at
	 * @param localOnly if you want to receive only local messages
	 * @param handler  the handler that will process the received messages
	 *
	 * @return the event bus message consumer
	 */
	public <T> MessageConsumer<T> consumer(String address, boolean localOnly, Handler<Message<T>> handler) {
		if (localOnly) {
			return vertx.eventBus().localConsumer(address, handler);
		} else {
			return vertx.eventBus().consumer(address, handler);
		}
	}

	public DeploymentOptions newDeploymentOpts() {
		return new DeploymentOptions().setWorkerPoolName("td-main-pool");
	}

	public SharedData getSharedData() {
		return vertx.sharedData();
	}

	public Mono<Void> close() {
		return Mono.from(vertx.rxClose().toFlowable()).then(Mono.fromRunnable(() -> {
			if (isMaster) {
				definedMasterCluster.set(false);
			}
		}));
	}
}
