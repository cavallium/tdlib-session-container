package it.tdlight.reactiveapi;

import static java.util.Objects.requireNonNull;

import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.LockSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class AtomixReactiveApi implements ReactiveApi {

	private static final Logger LOG = LoggerFactory.getLogger(AtomixReactiveApi.class);

	private final boolean clientOnly;

	private final KafkaTdlibClient kafkaTDLibClient;
	@Nullable
	private final KafkaTdlibServer kafkaTDLibServer;

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

	public AtomixReactiveApi(boolean clientOnly,
			KafkaParameters kafkaParameters,
			@Nullable DiskSessionsManager diskSessions,
			@NotNull Set<ResultingEventTransformer> resultingEventTransformerSet) {
		this.clientOnly = clientOnly;
		var kafkaTDLibRequestProducer = new KafkaTdlibRequestProducer(kafkaParameters);
		var kafkaTDLibResponseConsumer = new KafkaTdlibResponseConsumer(kafkaParameters);
		var kafkaClientBoundConsumer = new KafkaClientBoundConsumer(kafkaParameters);
		this.kafkaTDLibClient = new KafkaTdlibClient(kafkaTDLibRequestProducer,
				kafkaTDLibResponseConsumer,
				kafkaClientBoundConsumer
		);
		if (clientOnly) {
			this.kafkaTDLibServer = null;
		} else {
			var kafkaTDLibRequestConsumer = new KafkaTdlibRequestConsumer(kafkaParameters);
			var kafkaTDLibResponseProducer = new KafkaTdlibResponseProducer(kafkaParameters);
			var kafkaClientBoundProducer = new KafkaClientBoundProducer(kafkaParameters);
			this.kafkaTDLibServer = new KafkaTdlibServer(kafkaTDLibRequestConsumer,
					kafkaTDLibResponseProducer,
					kafkaClientBoundProducer
			);
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

		return idsSavedIntoLocalConfiguration
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
							true
					));
				})
				.then()
				.doOnTerminate(() -> LOG.info("Loaded all saved sessions from disk"));
	}

	@Override
	public Mono<CreateSessionResponse> createSession(CreateSessionRequest req) {
		LOG.debug("Received create session request: {}", req);

		if (clientOnly) {
			return Mono.error(new UnsupportedOperationException("This is a client, it can't have own sessions"));
		}

		// Create the session instance
		ReactiveApiPublisher reactiveApiPublisher;
		boolean loadedFromDisk;
		long userId;
		String botToken;
		Long phoneNumber;
		if (req instanceof CreateBotSessionRequest createBotSessionRequest) {
			loadedFromDisk = false;
			userId = createBotSessionRequest.userId();
			botToken = createBotSessionRequest.token();
			phoneNumber = null;
			reactiveApiPublisher = ReactiveApiPublisher.fromToken(kafkaTDLibServer, resultingEventTransformerSet,
					userId,
					botToken
			);
		} else if (req instanceof CreateUserSessionRequest createUserSessionRequest) {
			loadedFromDisk = false;
			userId = createUserSessionRequest.userId();
			botToken = null;
			phoneNumber = createUserSessionRequest.phoneNumber();
			reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(kafkaTDLibServer, resultingEventTransformerSet,
					userId,
					phoneNumber
			);
		} else if (req instanceof LoadSessionFromDiskRequest loadSessionFromDiskRequest) {
			loadedFromDisk = true;
			userId = loadSessionFromDiskRequest.userId();
			botToken = loadSessionFromDiskRequest.token();
			phoneNumber = loadSessionFromDiskRequest.phoneNumber();
			if (loadSessionFromDiskRequest.phoneNumber() != null) {
				reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(kafkaTDLibServer,
						resultingEventTransformerSet,
						userId,
						phoneNumber
				);
			} else {
				reactiveApiPublisher = ReactiveApiPublisher.fromToken(kafkaTDLibServer,
						resultingEventTransformerSet,
						userId,
						botToken
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
						var diskSession = new DiskSession(botToken, phoneNumber);
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
	public ReactiveApiClient client(String subGroupId, long userId) {
		return new LiveAtomixReactiveApiClient(kafkaTDLibClient, userId, subGroupId);
	}

	@Override
	public Mono<Void> close() {
		closeRequested = true;
		Mono<?> kafkaServerProducersStopper;
		if (kafkaTDLibServer != null) {
			kafkaServerProducersStopper = Mono.fromRunnable(kafkaTDLibServer::close).subscribeOn(Schedulers.boundedElastic());
		} else {
			kafkaServerProducersStopper = Mono.empty();
		}
		Mono<?> kafkaClientProducersStopper = Mono
				.fromRunnable(kafkaTDLibClient::close)
				.subscribeOn(Schedulers.boundedElastic());
		return Mono.when(kafkaServerProducersStopper, kafkaClientProducersStopper);
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
