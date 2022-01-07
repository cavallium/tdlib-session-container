package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.AuthPhase.*;
import static java.util.Objects.requireNonNull;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.Atomix;
import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.common.Response;
import it.tdlight.common.Signal;
import it.tdlight.common.utils.LibraryVersion;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.CheckAuthenticationBotToken;
import it.tdlight.jni.TdApi.CheckDatabaseEncryptionKey;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.PhoneNumberAuthenticationSettings;
import it.tdlight.jni.TdApi.SetAuthenticationPhoneNumber;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.TdlibParameters;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import it.tdlight.reactiveapi.Event.Request;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClusterBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ResultingEventPublisherClosed;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.tdlight.ClientManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.commons.lang3.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public abstract class ReactiveApiPublisher {


	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApiPublisher.class);
	private static final Duration SPECIAL_RAW_TIMEOUT_DURATION = Duration.ofSeconds(10);

	private final ClusterEventService eventService;
	private final ReactiveTelegramClient rawTelegramClient;
	private final Flux<Signal> telegramClient;

	private final AtomicReference<State> state = new AtomicReference<>(new State(LOGGED_OUT));
	protected final long userId;
	protected final long liveId;

	private final AtomicReference<Disposable> disposable = new AtomicReference<>();
	private final AtomicReference<Path> path = new AtomicReference<>();

	private ReactiveApiPublisher(Atomix atomix, long liveId, long userId) {
		this.userId = userId;
		this.liveId = liveId;
		this.rawTelegramClient = ClientManager.createReactive();
		this.telegramClient = Flux.<Signal>create(sink -> {
			rawTelegramClient.createAndRegisterClient();
			rawTelegramClient.setListener(sink::next);
			sink.onCancel(rawTelegramClient::cancel);
			sink.onDispose(rawTelegramClient::dispose);

			this.registerTopics();
		}).share();
		this.eventService = atomix.getEventService();
	}

	public static ReactiveApiPublisher fromToken(Atomix atomix, Long liveId, long userId, String token) {
		return new ReactiveApiPublisherToken(atomix, liveId, userId, token);
	}

	public static ReactiveApiPublisher fromPhoneNumber(Atomix atomix, Long liveId, long userId, long phoneNumber) {
		return new ReactiveApiPublisherPhoneNumber(atomix, liveId, userId, phoneNumber);
	}

	public void start(Path path, @Nullable Runnable onClose) {
		this.path.set(path);
		LOG.info("Starting session \"{}\" in path \"{}\"", this, path);
		var publishedResultingEvents = telegramClient
				.subscribeOn(Schedulers.parallel())
				// Handle signals, then return a ResultingEvent
				.mapNotNull(this::onSignal)
				.doFinally(s -> LOG.trace("Finalized telegram client events"))
				.publish();

		publishedResultingEvents
				// Obtain only TDLib-bound events
				.filter(s -> s instanceof TDLibBoundResultingEvent<?>)
				.map(s -> ((TDLibBoundResultingEvent<?>) s).action())
				// Buffer up to 64 requests to avoid halting the event loop, throw an error if too many requests are buffered
				.limitRate(4)
				.onBackpressureBuffer(64, BufferOverflowStrategy.ERROR)
				// Send requests to tdlib
				.concatMap(function -> Mono
						.from(rawTelegramClient.send(function, SPECIAL_RAW_TIMEOUT_DURATION))
						.mapNotNull(resp -> {
							if (resp.getConstructor() == TdApi.Error.CONSTRUCTOR) {
								LOG.error("Received error for special request {}: {}\nThe instance will be closed", function, resp);
								return new OnUpdateError(liveId, userId, (TdApi.Error) resp);
							} else {
								return null;
							}
						})
						.doOnError(ex -> LOG.error("Failed to receive the response for special request {}\n"
								+ " The instance will be closed", function, ex))
						.onErrorResume(ex -> Mono.just(new OnUpdateError(liveId, userId, new TdApi.Error(500, ex.getMessage()))))
				)
				.doOnError(ex -> LOG.error("Failed to receive resulting events. The instance will be closed", ex))
				.onErrorResume(ex -> Mono.just(new OnUpdateError(liveId, userId, new TdApi.Error(500, ex.getMessage()))))

				// when an error arrives, close the session
				.flatMap(ignored -> Mono
						.from(rawTelegramClient.send(new TdApi.Close(), Duration.ofMinutes(1)))
						.then(Mono.empty())
				)
				.subscribeOn(Schedulers.parallel())
				.subscribe();

		publishedResultingEvents
				// Obtain only client-bound events
				.filter(s -> s instanceof ClientBoundResultingEvent)
				.cast(ClientBoundResultingEvent.class)
				.map(ClientBoundResultingEvent::event)

				// Send events to the client
				.subscribeOn(Schedulers.parallel())
				.subscribe(clientBoundEvent -> eventService.broadcast("session-" + liveId + "-client-bound-events",
						clientBoundEvent, ReactiveApiPublisher::serializeEvent));

		publishedResultingEvents
				// Obtain only cluster-bound events
				.filter(s -> s instanceof ClusterBoundResultingEvent)
				.cast(ClusterBoundResultingEvent.class)

				// Send events to the cluster
				.subscribeOn(Schedulers.parallel())
				.subscribe(clusterBoundEvent -> {
					if (clusterBoundEvent instanceof ResultingEventPublisherClosed) {
						if (onClose != null) {
							onClose.run();
						}
					} else {
						LOG.error("Unknown cluster-bound event: {}", clusterBoundEvent);
					}
				});


		var prev = this.disposable.getAndSet(publishedResultingEvents.connect());
		if (prev != null) {
			LOG.error("The API started twice!");
			prev.dispose();
		}
	}

	@Nullable
	private ResultingEvent onSignal(Signal signal) {
		// Update the state
		var state = this.state.updateAndGet(oldState -> oldState.withSignal(signal));

		if (state.authPhase() == LOGGED_IN) {
			var update = (TdApi.Update) signal.getUpdate();
			return new ClientBoundResultingEvent(new OnUpdateData(liveId, userId, update));
		} else {
			LOG.trace("Signal has not been broadcast because the session {} is not logged in: {}", userId, signal);
			return this.handleSpecialSignal(state, signal);
		}
	}

	@SuppressWarnings("SwitchStatementWithTooFewBranches")
	@Nullable
	private ResultingEvent handleSpecialSignal(State state, Signal signal) {
		if (signal.isException()) {
			LOG.error("Received an error signal", signal.getException());
			return null;
		}
		if (signal.isClosed()) {
			signal.getClosed();
			LOG.info("Received a closed signal");
			return new ResultingEventPublisherClosed();
		}
		if (signal.isUpdate() && signal.getUpdate().getConstructor() == TdApi.Error.CONSTRUCTOR) {
			var error = ((TdApi.Error) signal.getUpdate());
			LOG.error("Received a TDLib error signal! Error {}: {}", error.code, error.message);
			return null;
		}
		if (!signal.isUpdate()) {
			LOG.error("Received a signal that's not an update: {}", signal);
			return null;
		}
		var update = signal.getUpdate();
		switch (state.authPhase()) {
			case BROKEN -> {}
			case PARAMETERS_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR -> {
								TdlibParameters parameters = generateTDLibParameters();
								return new TDLibBoundResultingEvent<>(new SetTdlibParameters(parameters));
							}
						}
					}
				}
			}
			case ENCRYPTION_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitEncryptionKey.CONSTRUCTOR -> {
								return new TDLibBoundResultingEvent<>(new CheckDatabaseEncryptionKey());
							}
						}
					}
				}
			}
			case AUTH_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR -> {
								return onWaitCode();
							}
							case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR -> {
								return new ClientBoundResultingEvent(new OnOtherDeviceLoginRequested(liveId, userId));
							}
							case TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR -> {
								return new ClientBoundResultingEvent(new OnPasswordRequested(liveId, userId));
							}
							case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR -> {
								return onWaitToken();
							}
						}
					}
				}
			}
		}
		return null;
	}

	private TdlibParameters generateTDLibParameters() {
		var tdlibParameters = new TdlibParameters();
		var path = requireNonNull(this.path.get(), "Path must not be null");
		tdlibParameters.databaseDirectory = path.resolve("database").toString();
		tdlibParameters.apiId = 94575;
		tdlibParameters.apiHash = "a3406de8d171bb422bb6ddf3bbd800e2";
		tdlibParameters.filesDirectory = path.resolve("files").toString();
		tdlibParameters.applicationVersion = it.tdlight.reactiveapi.generated.LibraryVersion.VERSION;
		tdlibParameters.deviceModel = System.getProperty("os.name");
		tdlibParameters.systemVersion = System.getProperty("os.version");
		tdlibParameters.enableStorageOptimizer = true;
		tdlibParameters.ignoreFileNames = true;
		tdlibParameters.useTestDc = false;
		tdlibParameters.useSecretChats = false;

		tdlibParameters.useMessageDatabase = true;
		tdlibParameters.useFileDatabase = true;
		tdlibParameters.useChatInfoDatabase = true;
		tdlibParameters.systemLanguageCode = System.getProperty("user.language", "en");
		return tdlibParameters;
	}

	protected abstract ResultingEvent onWaitToken();

	protected ResultingEvent onWaitCode() {
		LOG.error("Wait code event is not supported");
		return null;
	}

	private static byte[] serializeEvent(ClientBoundEvent clientBoundEvent) {
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				if (clientBoundEvent instanceof OnUpdateData onUpdateData) {
					dataOutputStream.writeByte(0x1);
					onUpdateData.update().serialize(dataOutputStream);
				} else if (clientBoundEvent instanceof OnUpdateError onUpdateError) {
					dataOutputStream.writeByte(0x2);
					onUpdateError.error().serialize(dataOutputStream);
				} else if (clientBoundEvent instanceof OnUserLoginCodeRequested onUserLoginCodeRequested) {
					dataOutputStream.writeByte(0x3);
					dataOutputStream.writeLong(onUserLoginCodeRequested.phoneNumber());
				} else if (clientBoundEvent instanceof OnBotLoginCodeRequested onBotLoginCodeRequested) {
					dataOutputStream.writeByte(0x4);
					dataOutputStream.writeUTF(onBotLoginCodeRequested.token());
				} else {
					throw new UnsupportedOperationException("Unexpected value: " + clientBoundEvent);
				}
				return byteArrayOutputStream.toByteArray();
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	private void registerTopics() {
		// Start receiving requests
		eventService.subscribe("session-" + liveId + "-requests",
				ReactiveApiPublisher::deserializeRequest,
				this::handleRequest,
				ReactiveApiPublisher::serializeResponse);
	}

	private static byte[] serializeResponse(Response response) {
		var id = response.getId();
		var object = response.getObject();
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				dataOutputStream.writeLong(id);
				object.serialize(dataOutputStream);
				return byteArrayOutputStream.toByteArray();
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	private CompletableFuture<Response> handleRequest(Request<Object> requestObj) {
		return Mono
				.just(requestObj)
				.filter(req -> {
					if (userId != req.liveId()) {
						LOG.error("Received a request for another session!");
						return false;
					} else {
						return true;
					}
				})
				.map(req -> new RequestWithTimeoutInstant<>(req.request(), req.timeout()))
				.flatMap(requestWithTimeoutInstant -> {
					var state = this.state.get();
					if (state.authPhase() == LOGGED_IN) {
						var request = requestWithTimeoutInstant.request();
						var timeoutDuration = Duration.between(Instant.now(), requestWithTimeoutInstant.timeout());
						if (timeoutDuration.isZero() || timeoutDuration.isNegative()) {
							LOG.error("Received an expired request. Expiration: {}", requestWithTimeoutInstant.timeout());
						}

						return Mono.from(rawTelegramClient.send(request, timeoutDuration));
					} else {
						LOG.error("Ignored a request because the current state is {}", state);
						return Mono.empty();
					}
				})
				.map(responseObj -> new Response(liveId, responseObj))
				.toFuture();
	}

	private static <T extends TdApi.Object> Request<T> deserializeRequest(byte[] bytes) {
		return Request.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ReactiveApiPublisher.class.getSimpleName() + "[", "]")
				.add("userId=" + userId)
				.add("liveId=" + liveId)
				.toString();
	}

	private record RequestWithTimeoutInstant<T extends TdApi.Object>(TdApi.Function<T> request, Instant timeout) {}

	private static class ReactiveApiPublisherToken extends ReactiveApiPublisher {

		private final String botToken;

		public ReactiveApiPublisherToken(Atomix atomix, Long liveId, long userId, String botToken) {
			super(atomix, liveId, userId);
			this.botToken = botToken;
		}

		@Override
		protected ResultingEvent onWaitToken() {
			return new TDLibBoundResultingEvent<>(new CheckAuthenticationBotToken(botToken));
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", ReactiveApiPublisherToken.class.getSimpleName() + "[", "]")
					.add("userId=" + userId)
					.add("liveId=" + liveId)
					.add("token='" + botToken + "'")
					.toString();
		}
	}

	private static class ReactiveApiPublisherPhoneNumber extends ReactiveApiPublisher {

		private final long phoneNumber;

		public ReactiveApiPublisherPhoneNumber(Atomix atomix, Long liveId, long userId, long phoneNumber) {
			super(atomix, liveId, userId);
			this.phoneNumber = phoneNumber;
		}

		@Override
		protected ResultingEvent onWaitToken() {
			var authSettings = new PhoneNumberAuthenticationSettings();
			authSettings.allowFlashCall = false;
			authSettings.allowSmsRetrieverApi = false;
			authSettings.isCurrentPhoneNumber = false;
			return new TDLibBoundResultingEvent<>(new SetAuthenticationPhoneNumber("+" + phoneNumber,
					authSettings
			));
		}

		@Override
		public ClientBoundResultingEvent onWaitCode() {
			return new ClientBoundResultingEvent(new OnUserLoginCodeRequested(liveId, userId, phoneNumber));
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", ReactiveApiPublisherPhoneNumber.class.getSimpleName() + "[", "]")
					.add("userId=" + userId)
					.add("liveId=" + liveId)
					.add("phoneNumber=" + phoneNumber)
					.toString();
		}
	}
}
