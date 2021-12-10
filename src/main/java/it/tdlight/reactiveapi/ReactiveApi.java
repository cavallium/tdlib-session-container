package it.tdlight.reactiveapi;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static reactor.core.publisher.Mono.fromCompletionStage;

import io.atomix.core.Atomix;
import io.atomix.core.idgenerator.AsyncAtomicIdGenerator;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.protocols.raft.MultiRaftProtocol;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ReactiveApi {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApi.class);

	@NotNull
	private final String nodeId;
	private final Atomix atomix;
	private final AsyncAtomicIdGenerator nextSessionLiveId;

	private final AsyncAtomicLock sessionModificationLock;
	private final AsyncAtomicMap<Long, String> userIdToNodeId;
	/**
	 * User id -> session
	 */
	private final ConcurrentMap<Long, ReactiveApiPublisher> localLiveSessions = new ConcurrentHashMap<>();
	private final DiskSessionsManager diskSessions;

	public ReactiveApi(@NotNull String nodeId, Atomix atomix, DiskSessionsManager diskSessions) {
		this.nodeId = nodeId;
		this.atomix = atomix;

		var raftProtocol = MultiRaftProtocol.builder().build();
		this.nextSessionLiveId = atomix
				.atomicIdGeneratorBuilder("session-live-id")
				.withProtocol(raftProtocol)
				.build()
				.async();
		this.sessionModificationLock = atomix
				.atomicLockBuilder("session-modification")
				.withProtocol(raftProtocol)
				.build()
				.async();

		this.userIdToNodeId = atomix
				.<Long, String>atomicMapBuilder("user-id-to-node-id")
				//.withCacheEnabled(true)
				//.withCacheSize(4096)
				.withNullValues(false)
				.withProtocol(raftProtocol)
				.build()
				.async();

		this.diskSessions = diskSessions;
	}

	public Mono<Void> start() {
		Mono<Set<Long>> idsSavedIntoLocalConfiguration = Mono.fromCallable(() -> {
			synchronized (diskSessions) {
				return diskSessions.getSettings().userIdToSession().keySet();
			}
		});
		Mono<Set<Long>> distributedIds = this
				.getAllUsers()
				.flatMapIterable(Map::entrySet)
				.filter(entry -> entry.getValue().equals(nodeId))
				.map(Entry::getKey)
				.collect(Collectors.toUnmodifiableSet());

		record DiskChanges(Set<Long> normalIds, Set<Long> addedIds, Set<Long> removedIds) {}

		var diskChangesMono = Mono.zip(idsSavedIntoLocalConfiguration, distributedIds).map(tuple -> {
			var localSet = tuple.getT1();
			var remoteSet = tuple.getT2();

			var deletedUsers = new HashSet<>(remoteSet);
			deletedUsers.removeAll(localSet);

			var addedUsers = new HashSet<>(localSet);
			addedUsers.removeAll(remoteSet);

			var normalUsers = new HashSet<>(localSet);
			normalUsers.removeAll(addedUsers);

			for (long user : addedUsers) {
				LOG.warn("Detected a new user id from the disk configuration file: {}", user);
			}
			for (long user : normalUsers) {
				LOG.info("Detected a user id from the disk configuration file: {}", user);
			}
			for (long user : deletedUsers) {
				LOG.warn("The user id {} has been deleted from the disk configuration file", user);
			}

			return new DiskChanges(unmodifiableSet(normalUsers), unmodifiableSet(addedUsers), unmodifiableSet(remoteSet));
		}).cache();

		var removeObsoleteDiskSessions = diskChangesMono
				.flatMapIterable(diskChanges -> diskChanges.removedIds)
				.flatMap(removedIds -> fromCompletionStage(() -> destroySession(removedIds, nodeId)))
				.then();

		var addedDiskSessionsFlux = diskChangesMono
				.flatMapIterable(diskChanges -> diskChanges.addedIds)
				.flatMap(this::getLocalDiskSession);
		var normalDiskSessionsFlux = diskChangesMono
				.flatMapIterable(diskChanges -> diskChanges.normalIds)
				.flatMap(this::getLocalDiskSession);

		var addNewDiskSessions = addedDiskSessionsFlux.flatMap(diskSessionAndId -> {
			var id = diskSessionAndId.id;
			var diskSession = diskSessionAndId.diskSession;
			return createSession(new LoadSessionFromDiskRequest(id,
					diskSession.token,
					diskSession.phoneNumber,
					true
			));
		}).then();

		var loadExistingDiskSessions = normalDiskSessionsFlux.flatMap(diskSessionAndId -> {
			var id = diskSessionAndId.id;
			var diskSession = diskSessionAndId.diskSession;
			return createSession(new LoadSessionFromDiskRequest(id,
					diskSession.token,
					diskSession.phoneNumber,
					false
			));
		}).then();

		var diskInitMono = Mono.when(removeObsoleteDiskSessions, loadExistingDiskSessions, addNewDiskSessions)
				.subscribeOn(Schedulers.boundedElastic())
				.doOnTerminate(() -> LOG.info("Loaded all saved sessions from disk"));

		// Listen for create-session signals
		var subscriptionMono = fromCompletionStage(() -> atomix
				.getEventService()
				.subscribe("create-session", CreateSessionRequest::deserializeBytes, req -> {
					if (req instanceof LoadSessionFromDiskRequest) {
						return failedFuture(new IllegalArgumentException("Can't pass a local request through the cluster"));
					} else {
						return createSession(req).toFuture();
					}
				}, CreateSessionResponse::serializeBytes));

		return diskInitMono.then(subscriptionMono).then();
	}

	private CompletableFuture<Void> destroySession(long userId, String nodeId) {
		LOG.debug("Received session delete request: user_id={}, node_id=\"{}\"", userId, nodeId);

		// Lock sessions modification
		return sessionModificationLock
				.lock()
				.thenCompose(lockVersion -> {
					LOG.trace("Obtained session modification lock for session delete request: {} \"{}\"", userId, nodeId);
					return userIdToNodeId
							.remove(userId, nodeId)
							.thenAccept(deleted -> LOG.debug("Deleted session {} \"{}\": {}", userId, nodeId, deleted));
				})
				.whenComplete((response, error) -> sessionModificationLock
						.unlock()
						.thenRun(() -> LOG.trace("Released session modification lock for session delete request: {} \"{}\"", userId, nodeId))
				)
				.whenComplete((resp, ex) -> LOG.debug("Handled session delete request {} \"{}\", the response is: {}", userId, nodeId, resp, ex));
	}

	public Mono<CreateSessionResponse> createSession(CreateSessionRequest req) {
		LOG.debug("Received create session request: {}", req);

		Mono<CreateSessionResponse> unlockedSessionCreationMono = Mono.defer(() -> {
			LOG.trace("Obtained session modification lock for session request: {}", req);
			// Generate session id
			return this
					.nextFreeLiveId()
					.flatMap(liveId -> {
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
							reactiveApiPublisher = ReactiveApiPublisher.fromToken(atomix, liveId, userId, botToken);
						} else if (req instanceof CreateUserSessionRequest createUserSessionRequest) {
							loadedFromDisk = false;
							userId = createUserSessionRequest.userId();
							botToken = null;
							phoneNumber = createUserSessionRequest.phoneNumber();
							reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(atomix, liveId, userId, phoneNumber);
						} else if (req instanceof LoadSessionFromDiskRequest loadSessionFromDiskRequest) {
							loadedFromDisk = true;
							userId = loadSessionFromDiskRequest.userId();
							botToken = loadSessionFromDiskRequest.token();
							phoneNumber = loadSessionFromDiskRequest.phoneNumber();
							if (loadSessionFromDiskRequest.phoneNumber() != null) {
								reactiveApiPublisher = ReactiveApiPublisher.fromPhoneNumber(atomix, liveId, userId, phoneNumber);
							} else {
								reactiveApiPublisher = ReactiveApiPublisher.fromToken(atomix, liveId, userId, botToken);
							}
						} else {
							return Mono.error(new UnsupportedOperationException("Unexpected value: " + req));
						}

						// Register the session instance to the local nodes map
						var prev = localLiveSessions.put(liveId, reactiveApiPublisher);
						if (prev != null) {
							LOG.error("User id \"{}\" was already registered locally! {}", liveId, prev);
						}

						// Register the session instance to the distributed nodes map
						return Mono
								.fromCompletionStage(() -> userIdToNodeId.put(userId, nodeId))
								.flatMap(prevDistributed -> {
									if (prevDistributed != null && prevDistributed.value() != null &&
											!Objects.equals(this.nodeId, prevDistributed.value())) {
										LOG.error("Session id \"{}\" is already registered in the node \"{}\"!", liveId, prevDistributed.value());
									}

									var saveToDiskMono = Mono
											.<Void>fromCallable(() -> {
												// Save updated sessions configuration to disk
												try {
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
												synchronized (diskSessions) {
													return Paths.get(diskSessions.getSettings().path);
												}
											})
											.subscribeOn(Schedulers.boundedElastic())
											.flatMap(baseSessionsPath -> {
												String diskSessionFolderName = Long.toUnsignedString(userId);
												Path sessionPath = baseSessionsPath.resolve(diskSessionFolderName);

												if (!loadedFromDisk) {
													// Create the disk session configuration
													var diskSession = new DiskSession(botToken, phoneNumber);
													return Mono.<Void>fromCallable(() -> {
														synchronized (diskSessions) {
															diskSessions.getSettings().userIdToSession().put(userId, diskSession);
															return null;
														}
													}).subscribeOn(Schedulers.boundedElastic()).then(saveToDiskMono).thenReturn(sessionPath);
												} else {
													return Mono.just(sessionPath);
												}
											})
											.doOnNext(reactiveApiPublisher::start)
											.thenReturn(new CreateSessionResponse(liveId));
								});
					});
		});

		// Lock sessions creation
		return Mono
				.usingWhen(Mono.fromCompletionStage(sessionModificationLock::lock),
						lockVersion -> unlockedSessionCreationMono,
						lockVersion -> Mono
								.fromCompletionStage(sessionModificationLock::unlock)
								.doOnTerminate(() -> LOG.trace("Released session modification lock for session request: {}", req))
				)
				.doOnNext(resp -> LOG.debug("Handled session request {}, the response is: {}", req, resp))
				.doOnError(ex -> LOG.debug("Handled session request {}, the response is: error", req, ex));
	}

	public Mono<Long> nextFreeLiveId() {
		return Mono.fromCompletionStage(nextSessionLiveId::nextId);
	}

	public Atomix getAtomix() {
		return atomix;
	}

	/**
	 * Get the list of current sessions
	 * @return map of user id -> node id
	 */
	public Mono<Map<Long, String>> getAllUsers() {
		return Flux.defer(() -> {
			var it = userIdToNodeId.entrySet().iterator();
			var hasNextMono = fromCompletionStage(it::hasNext);
			var strictNextMono = fromCompletionStage(it::next)
					.map(elem -> Map.entry(elem.getKey(), elem.getValue().value()));

			var nextOrNothingMono = hasNextMono.flatMap(hasNext -> {
				if (hasNext) {
					return strictNextMono;
				} else {
					return Mono.empty();
				}
			});
			return nextOrNothingMono.repeatWhen(s -> s.takeWhile(n -> n > 0));
		}).collectMap(Entry::getKey, Entry::getValue);
	}

	public boolean is(String nodeId) {
		return this.nodeId.equals(nodeId);
	}

	private record DiskSessionAndId(DiskSession diskSession, long id) {}

	private Mono<DiskSessionAndId> getLocalDiskSession(Long localId) {
		return Mono.fromCallable(() -> {
			synchronized (diskSessions) {
				var diskSession = requireNonNull(diskSessions.getSettings().userIdToSession().get(localId),
						"Id not found: " + localId
				);
				try {
					diskSession.validate();
				} catch (Throwable ex) {
					LOG.error("Failed to load disk session {}", localId, ex);
					return null;
				}
				return new DiskSessionAndId(diskSession, localId);
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}
}