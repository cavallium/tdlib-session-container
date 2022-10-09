package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNull;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class AtomixReactiveApi implements ReactiveApi {

	private static final Logger LOG = LogManager.getLogger(AtomixReactiveApi.class);

	private final AtomixReactiveApiMode mode;

	private final TdlibChannelsSharedReceive sharedTdlibClients;
	@Nullable
	private final TdlibChannelsSharedHost sharedTdlibServers;
	private final ReactiveApiMultiClient client;

	private final Set<ResultingEventTransformer> resultingEventTransformerSet;
	/**
	 * user id -> session
	 */
	private final ConcurrentMap<Long, ReactiveApiPublisher> localSessions = new ConcurrentHashMap<>();
	/**
	 * DiskSessions is null when nodeId is null
	 */
	@Nullable
	private final DiskSessionsManager diskSessions;
	private volatile boolean closeRequested;
	private volatile Disposable requestsSub;

	public enum AtomixReactiveApiMode {
		CLIENT,
		SERVER,
		FULL
	}

	public AtomixReactiveApi(AtomixReactiveApiMode mode,
			ChannelsParameters channelsParameters,
			@Nullable DiskSessionsManager diskSessions,
			@NotNull Set<ResultingEventTransformer> resultingEventTransformerSet) {
		this.mode = mode;
		ChannelFactory channelFactory = ChannelFactory.getFactoryFromParameters(channelsParameters);
		if (mode != AtomixReactiveApiMode.SERVER) {
			EventProducer<OnRequest<?>> tdRequestProducer = ChannelProducerTdlibRequest.create(channelFactory);
			EventConsumer<OnResponse<Object>> tdResponseConsumer = ChannelConsumerTdlibResponse.create(channelFactory);
			HashMap<String, EventConsumer<ClientBoundEvent>> clientBoundConsumers = new HashMap<>();
			for (String lane : channelsParameters.getAllLanes()) {
				clientBoundConsumers.put(lane, ChannelConsumerClientBoundEvent.create(channelFactory, lane));
			}
			var tdClientsChannels = new TdlibChannelsClients(tdRequestProducer,
					tdResponseConsumer,
					clientBoundConsumers
			);
			this.sharedTdlibClients = new TdlibChannelsSharedReceive(tdClientsChannels);
			this.client = new LiveAtomixReactiveApiClient(sharedTdlibClients);
		} else {
			this.sharedTdlibClients = null;
			this.client = null;
		}
		if (mode != AtomixReactiveApiMode.CLIENT) {
			EventConsumer<OnRequest<Object>> tdRequestConsumer = ChannelConsumerTdlibRequest.create(channelFactory);
			EventProducer<OnResponse<Object>> tdResponseProducer = ChannelProducerTdlibResponse.create(channelFactory);
			var clientBoundProducers = new HashMap<String, EventProducer<ClientBoundEvent>>();
			for (String lane : channelsParameters.getAllLanes()) {
				clientBoundProducers.put(lane, ChannelProducerClientBoundEvent.create(channelFactory, lane));
			}
			var tdServer = new TdlibChannelsServers(tdRequestConsumer,
					tdResponseProducer,
					clientBoundProducers
			);
			this.sharedTdlibServers = new TdlibChannelsSharedHost(channelsParameters.getAllLanes(), tdServer);
		} else {
			this.sharedTdlibServers = null;
		}
		this.resultingEventTransformerSet = resultingEventTransformerSet;

		this.diskSessions = diskSessions;
	}

	@Override
	public Mono<Void> start() {
		var idsSavedIntoLocalConfiguration = Mono
				.<Set<Entry<Long, DiskSession>>>fromCallable(() -> {
					if (diskSessions == null) {
						return Set.of();
					}
					synchronized (diskSessions) {
						return diskSessions.getSettings().userIdToSession().entrySet();
					}
				})
				.subscribeOn(Schedulers.boundedElastic())
				.flatMapIterable(a -> a)
				.map(a -> new DiskSessionAndId(a.getValue(), a.getKey()));

		var loadSessions = idsSavedIntoLocalConfiguration
				.filter(diskSessionAndId -> {
					try {
						diskSessionAndId.diskSession().validate();
					} catch (Throwable ex) {
						LOG.error("Failed to load disk session {}", diskSessionAndId.id, ex);
						return false;
					}
					return true;
				})
				.flatMap(diskSessionAndId -> {
					var id = diskSessionAndId.id;
					var diskSession = diskSessionAndId.diskSession;
					return createSession(new LoadSessionFromDiskRequest(id,
							diskSession.token,
							diskSession.phoneNumber,
							diskSession.lane,
							true
					));
				})
				.then()
				.doOnTerminate(() -> LOG.info("Loaded all saved sessions from disk"));

		return loadSessions.<Void>then(Mono.fromRunnable(() -> {
			if (sharedTdlibServers != null) {
				requestsSub = sharedTdlibServers.requests()
						.doOnNext(req -> localSessions.get(req.data().userId()).handleRequest(req.data()))
						.subscribeOn(Schedulers.parallel())
						.subscribe(n -> {}, ex -> LOG.error("Requests channel broke unexpectedly", ex));
			}
			})).transform(ReactorUtils::subscribeOnceUntilUnsubscribe);
	}

	@Override
	public Mono<CreateSessionResponse> createSession(CreateSessionRequest req) {
		LOG.debug("Received create session request: {}", req);

		if (mode == AtomixReactiveApiMode.CLIENT) {
			return Mono.error(new UnsupportedOperationException("This is a client, it can't have own sessions"));
		}

		// Create the session instance
		ReactiveApiPublisher reactiveApiPublisher;
		boolean loadedFromDisk;
		long userId;
		String botToken;
		String lane;
		Long phoneNumber;
		if (req instanceof CreateBotSessionRequest createBotSessionRequest) {
			loadedFromDisk = false;
			userId = createBotSessionRequest.userId();
			botToken = createBotSessionRequest.token();
			phoneNumber = null;
			lane = createBotSessionRequest.lane();
			reactiveApiPublisher = ReactiveApiPublisher.fromToken(sharedTdlibServers, resultingEventTransformerSet,
					userId,
					botToken,
					lane
			);
		} else if (req instanceof CreateUserSessionRequest createUserSessionRequest) {
			loadedFromDisk = false;
			userId = createUserSessionRequest.userId();
			botToken = null;
			phoneNumber = createUserSessionRequest.phoneNumber();
			lane = createUserSessionRequest.lane();
			reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(sharedTdlibServers, resultingEventTransformerSet,
					userId,
					phoneNumber,
					lane
			);
		} else if (req instanceof LoadSessionFromDiskRequest loadSessionFromDiskRequest) {
			loadedFromDisk = true;
			userId = loadSessionFromDiskRequest.userId();
			botToken = loadSessionFromDiskRequest.token();
			phoneNumber = loadSessionFromDiskRequest.phoneNumber();
			lane = loadSessionFromDiskRequest.lane();
			if (loadSessionFromDiskRequest.phoneNumber() != null) {
				reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(sharedTdlibServers,
						resultingEventTransformerSet,
						userId,
						phoneNumber,
						lane
				);
			} else {
				reactiveApiPublisher = ReactiveApiPublisher.fromToken(sharedTdlibServers,
						resultingEventTransformerSet,
						userId,
						botToken,
						lane
				);
			}
		} else {
			return Mono.error(new UnsupportedOperationException("Unexpected value: " + req));
		}

		// Register the session instance to the local nodes map
		var prev = localSessions.put(userId, reactiveApiPublisher);
		if (prev != null) {
			LOG.error("User id \"{}\" was already registered locally! {}", userId, prev);
		}

		var saveToDiskMono = Mono
				.<Void>fromCallable(() -> {
					// Save updated sessions configuration to disk
					try {
						Objects.requireNonNull(diskSessions);

						synchronized (diskSessions) {
							diskSessions.save();
							return null;
						}
					} catch (IOException e) {
						throw new CompletionException("Failed to save disk sessions configuration", e);
					}
				})
				.subscribeOn(Schedulers.boundedElastic());

		// Start the session instance
		return Mono
				.fromCallable(() -> {
					Objects.requireNonNull(diskSessions);
					synchronized (diskSessions) {
						return Objects.requireNonNull(Paths.get(diskSessions.getSettings().path),
								"Session " + userId + " path is missing");
					}
				})
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(baseSessionsPath -> {
					String diskSessionFolderName = "id" + Long.toUnsignedString(userId);
					Path sessionPath = baseSessionsPath.resolve(diskSessionFolderName);

					if (!loadedFromDisk) {
						// Create the disk session configuration
						var diskSession = new DiskSession(botToken, phoneNumber, lane);
						return Mono.<Void>fromCallable(() -> {
							Objects.requireNonNull(diskSessions);
							synchronized (diskSessions) {
								diskSessions.getSettings().userIdToSession().put(userId, diskSession);
								return null;
							}
						}).subscribeOn(Schedulers.boundedElastic()).then(saveToDiskMono).thenReturn(sessionPath);
					} else {
						return Mono.just(sessionPath);
					}
				})
				.doOnNext(path -> reactiveApiPublisher.start(path, () -> {
					localSessions.remove(userId);
					LOG.debug("Closed the session for user {} after it was closed itself", userId);
				}))
				.thenReturn(new CreateSessionResponse(userId));
	}

	@Override
	public ReactiveApiMultiClient client() {
		return client;
	}

	@Override
	public Mono<Void> close() {
		closeRequested = true;
		Mono<?> serverProducersStopper;
		if (sharedTdlibServers != null) {
			serverProducersStopper = Mono.fromRunnable(sharedTdlibServers::close).subscribeOn(Schedulers.boundedElastic());
		} else {
			serverProducersStopper = Mono.empty();
		}
		Mono<?> clientProducersStopper;
		if (sharedTdlibClients != null) {
			clientProducersStopper = Mono
					.fromRunnable(sharedTdlibClients::close)
					.subscribeOn(Schedulers.boundedElastic());
		} else {
			clientProducersStopper = Mono.empty();
		}
		if (requestsSub != null) {
			requestsSub.dispose();
		}
		return Mono.when(serverProducersStopper, clientProducersStopper);
	}

	@Override
	public void waitForExit() {
		var nanos = Duration.ofSeconds(1).toNanos();
		while (!closeRequested && !Thread.interrupted()) {
			LockSupport.parkNanos(nanos);
		}
	}

	private record DiskSessionAndId(DiskSession diskSession, long id) {}

	private Mono<DiskSessionAndId> getLocalDiskSession(Long localUserId) {
		return Mono.fromCallable(() -> {
			Objects.requireNonNull(diskSessions);
			synchronized (diskSessions) {
				var diskSession = requireNonNull(diskSessions.getSettings().userIdToSession().get(localUserId),
						"Id not found: " + localUserId
				);
				try {
					diskSession.validate();
				} catch (Throwable ex) {
					LOG.error("Failed to load disk session {}", localUserId, ex);
					return null;
				}
				return new DiskSessionAndId(diskSession, localUserId);
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}
}
