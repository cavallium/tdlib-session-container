package it.tdlight.reactiveapi;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import io.atomix.core.Atomix;
import io.atomix.core.idgenerator.AsyncAtomicIdGenerator;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.map.AsyncAtomicMap;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

public class ReactiveApi {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApi.class);

	private final Atomix atomix;
	private static final SchedulerExecutor SCHEDULER_EXECUTOR = new SchedulerExecutor(Schedulers.parallel());
	private static final SchedulerExecutor BOUNDED_ELASTIC_EXECUTOR = new SchedulerExecutor(Schedulers.boundedElastic());
	private final AsyncAtomicIdGenerator nextSessionId;

	private final AsyncAtomicLock sessionModificationLock;
	private final AsyncAtomicMap<Long, Long> sessionIdToUserId;
	private final ConcurrentMap<Long, ReactiveApiPublisher> localNodeSessions = new ConcurrentHashMap<>();
	private final DiskSessionsManager diskSessions;

	public ReactiveApi(Atomix atomix, DiskSessionsManager diskSessions) {
		this.atomix = atomix;
		this.nextSessionId = atomix.getAtomicIdGenerator("session-id").async();
		this.sessionIdToUserId = atomix.<Long, Long>getAtomicMap("session-id-to-user-id").async();
		this.sessionModificationLock = atomix.getAtomicLock("session-modification").async();
		this.diskSessions = diskSessions;
	}

	public void start() {
		CompletableFuture.runAsync(() -> {
			List<CompletableFuture<CreateSessionResponse>> requests = new ArrayList<>();
			synchronized (diskSessions) {
				for (Entry<String, DiskSession> entry : diskSessions.getSettings().sessions.entrySet()) {
					try {
						entry.getValue().validate();
					} catch (Throwable ex) {
						LOG.error("Failed to load disk session {}", entry.getKey(), ex);
					}
					var sessionFolderName = entry.getKey();
					var diskSession = entry.getValue();
					requests.add(createSession(new LoadSessionFromDiskRequest(diskSession.userId,
							sessionFolderName,
							diskSession.token,
							diskSession.phoneNumber
					)));
				}
			}
			CompletableFuture
					.allOf(requests.toArray(CompletableFuture<?>[]::new))
					.thenAccept(responses -> LOG.info("Loaded all saved sessions from disk"));
		}, BOUNDED_ELASTIC_EXECUTOR);

		// Listen for create-session signals
		atomix.getEventService().subscribe("create-session", CreateSessionRequest::deserializeBytes, req -> {
			if (req instanceof LoadSessionFromDiskRequest) {
				return failedFuture(new IllegalArgumentException("Can't pass a local request through the cluster"));
			} else {
				return createSession(req);
			}
		}, CreateSessionResponse::serializeBytes);
	}

	public CompletableFuture<CreateSessionResponse> createSession(CreateSessionRequest req) {
		// Lock sessions creation
		return sessionModificationLock.lock().thenCompose(lockVersion -> {
			// Generate session id
			return this.nextFreeId().thenCompose(sessionId -> {
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
					reactiveApiPublisher = ReactiveApiPublisher.fromToken(atomix, sessionId, userId, botToken);
				} else if (req instanceof CreateUserSessionRequest createUserSessionRequest) {
					loadedFromDisk = false;
					userId = createUserSessionRequest.userId();
					botToken = null;
					phoneNumber = createUserSessionRequest.phoneNumber();
					reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(atomix, sessionId, userId, phoneNumber);
				} else if (req instanceof LoadSessionFromDiskRequest loadSessionFromDiskRequest) {
					loadedFromDisk = true;
					userId = loadSessionFromDiskRequest.userId();
					botToken = loadSessionFromDiskRequest.token();
					phoneNumber = loadSessionFromDiskRequest.phoneNumber();
					if (loadSessionFromDiskRequest.phoneNumber() != null) {
						reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(atomix, sessionId, userId, phoneNumber);
					} else {
						reactiveApiPublisher = ReactiveApiPublisher.fromToken(atomix, sessionId, userId, botToken);
					}
				} else {
					return failedFuture(new UnsupportedOperationException("Unexpected value: " + req));
				}

				// Register the session instance to the local nodes map
				var prev = localNodeSessions.put(sessionId, reactiveApiPublisher);
				if (prev != null) {
					LOG.error("Session id \"{}\" was already registered locally!", sessionId);
				}

				// Register the session instance to the distributed nodes map
				return sessionIdToUserId.put(sessionId, req.userId()).thenComposeAsync(prevDistributed -> {
					if (prevDistributed != null) {
						LOG.error("Session id \"{}\" was already registered in the cluster!", sessionId);
					}

					CompletableFuture<?> saveToDiskFuture;
					if (!loadedFromDisk) {
						// Load existing session paths
						HashSet<String> alreadyExistingPaths = new HashSet<>();
						synchronized (diskSessions) {
							for (var entry : diskSessions.getSettings().sessions.entrySet()) {
								var path = entry.getKey();
								var diskSessionSettings = entry.getValue();
								if (diskSessionSettings.userId == userId) {
									LOG.warn("User id \"{}\" session already exists in path: \"{}\"", userId, path);
								}
								alreadyExistingPaths.add(entry.getKey());
							}
						}

						// Get a new disk session folder name
						String diskSessionFolderName;
						do {
							diskSessionFolderName = UUID.randomUUID().toString();
						} while (alreadyExistingPaths.contains(diskSessionFolderName));

						// Create the disk session configuration
						var diskSession = new DiskSession(userId, botToken, phoneNumber);
						Path path;
						synchronized (diskSessions) {
							diskSessions.getSettings().sessions.put(diskSessionFolderName, diskSession);
							path = Paths.get(diskSessions.getSettings().path).resolve(diskSessionFolderName);
						}

						// Start the session instance
						reactiveApiPublisher.start(path);

						saveToDiskFuture = CompletableFuture.runAsync(() -> {
							// Save updated sessions configuration to disk
							try {
								synchronized (diskSessions) {
									diskSessions.save();
								}
							} catch (IOException e) {
								throw new CompletionException("Failed to save disk sessions configuration", e);
							}
						}, BOUNDED_ELASTIC_EXECUTOR);
					} else {
						saveToDiskFuture = completedFuture(null);
					}

					return saveToDiskFuture.thenApply(ignored -> new CreateSessionResponse(sessionId));
				}, BOUNDED_ELASTIC_EXECUTOR);
			});
		});
	}

	public CompletableFuture<Long> nextFreeId() {
		return nextSessionId.nextId().thenCompose(id -> sessionIdToUserId.containsKey(id).thenCompose(exists -> {
			if (exists) {
				return nextFreeId();
			} else {
				return completedFuture(id);
			}
		}));
	}

	public Atomix getAtomix() {
		return atomix;
	}
}
