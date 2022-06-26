package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.AuthPhase.LOGGED_IN;
import static it.tdlight.reactiveapi.AuthPhase.LOGGED_OUT;
import static it.tdlight.reactiveapi.Event.SERIAL_VERSION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.google.common.primitives.Longs;
import it.tdlight.common.Init;
import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.common.Response;
import it.tdlight.common.Signal;
import it.tdlight.common.utils.CantLoadLibrary;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.AuthorizationStateWaitOtherDeviceConfirmation;
import it.tdlight.jni.TdApi.AuthorizationStateWaitPassword;
import it.tdlight.jni.TdApi.CheckAuthenticationBotToken;
import it.tdlight.jni.TdApi.CheckDatabaseEncryptionKey;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.PhoneNumberAuthenticationSettings;
import it.tdlight.jni.TdApi.SetAuthenticationPhoneNumber;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.TdlibParameters;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnRequest.Request;
import it.tdlight.reactiveapi.Event.OnResponse;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.SerializationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public abstract class ReactiveApiPublisher {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApiPublisher.class);
	private static final Duration SPECIAL_RAW_TIMEOUT_DURATION = Duration.ofMinutes(5);

	private static final Duration TEN_MS = Duration.ofMillis(10);

	private final KafkaTdlibServer kafkaTdlibServer;
	private final Set<ResultingEventTransformer> resultingEventTransformerSet;
	private final ReactiveTelegramClient rawTelegramClient;
	private final Flux<Signal> telegramClient;

	private final AtomicReference<State> state = new AtomicReference<>(new State(LOGGED_OUT));
	protected final long userId;

	private final Many<OnResponse<TdApi.Object>> responses
			= Sinks.many().unicast().onBackpressureBuffer(Queues.<OnResponse<TdApi.Object>>small().get());

	private final AtomicReference<Disposable> disposable = new AtomicReference<>();
	private final AtomicReference<Path> path = new AtomicReference<>();

	private ReactiveApiPublisher(KafkaTdlibServer kafkaTdlibServer,
			Set<ResultingEventTransformer> resultingEventTransformerSet,
			long userId) {
		this.kafkaTdlibServer = kafkaTdlibServer;
		this.resultingEventTransformerSet = resultingEventTransformerSet;
		this.userId = userId;
		this.rawTelegramClient = ClientManager.createReactive();
		try {
			Init.start();
		} catch (CantLoadLibrary e) {
			throw new RuntimeException("Can't load TDLight", e);
		}
		this.telegramClient = Flux.<Signal>create(sink -> {
			var subscription = this.registerTopics();
			try {
				rawTelegramClient.createAndRegisterClient();
			} catch (Throwable ex) {
				LOG.error("Failed to initialize client {}", userId, ex);
				sink.error(ex);
			}
			rawTelegramClient.setListener(sink::next);
			sink.onCancel(rawTelegramClient::cancel);
			sink.onDispose(() -> {
				subscription.dispose();
				rawTelegramClient.dispose();
			});
		});
	}

	public static ReactiveApiPublisher fromToken(KafkaTdlibServer kafkaTdlibServer,
			Set<ResultingEventTransformer> resultingEventTransformerSet,
			long userId,
			String token) {
		return new ReactiveApiPublisherToken(kafkaTdlibServer, resultingEventTransformerSet, userId, token);
	}

	public static ReactiveApiPublisher fromPhoneNumber(KafkaTdlibServer kafkaTdlibServer,
			Set<ResultingEventTransformer> resultingEventTransformerSet,
			long userId,
			long phoneNumber) {
		return new ReactiveApiPublisherPhoneNumber(kafkaTdlibServer,
				resultingEventTransformerSet,
				userId,
				phoneNumber
		);
	}

	public void start(Path path, @Nullable Runnable onClose) {
		this.path.set(path);
		LOG.info("Starting session \"{}\" in path \"{}\"", this, path);
		var publishedResultingEvents = telegramClient
				.subscribeOn(Schedulers.parallel())
				// Handle signals, then return a ResultingEvent
				.concatMapIterable(this::onSignal)
				.doFinally(s -> LOG.trace("Finalized telegram client events"))

				// Transform resulting events using all the registered resulting event transformers
				.transform(flux -> {
					Flux<ResultingEvent> transformedFlux = flux;
					for (ResultingEventTransformer resultingEventTransformer : resultingEventTransformerSet) {
						transformedFlux = resultingEventTransformer.transform(isBot(), transformedFlux);
					}
					return transformedFlux;
				})

				.publish(256);

		publishedResultingEvents
				// Obtain only TDLib-bound events
				.filter(s -> s instanceof TDLibBoundResultingEvent<?>)
				.map(s -> ((TDLibBoundResultingEvent<?>) s))

				// Buffer requests to avoid halting the event loop
				.onBackpressureBuffer()

				// Send requests to tdlib
				.flatMap(req -> Mono
						.from(rawTelegramClient.send(req.action(), SPECIAL_RAW_TIMEOUT_DURATION))
						.flatMap(result -> fixBrokenKey(req.action(), result))
						.mapNotNull(resp -> {
							if (resp.getConstructor() == TdApi.Error.CONSTRUCTOR) {
								if (req.ignoreFailure()) {
									LOG.debug("Received error for special request {}", req.action());
									return null;
								} else {
									LOG.error("Received error for special request {}: {}\nThe instance will be closed", req.action(), resp);
									return new OnUpdateError(userId, (TdApi.Error) resp);
								}
							} else {
								return null;
							}
						})
						.doOnError(ex -> LOG.error("Failed to receive the response for special request {}\n"
								+ " The instance will be closed", req.action(), ex))
						.onErrorResume(ex -> Mono.just(new OnUpdateError(userId, new TdApi.Error(500, ex.getMessage()))))
				, Integer.MAX_VALUE)

				// Buffer requests to avoid halting the event loop
				.onBackpressureBuffer()

				.doOnError(ex -> LOG.error("Failed to receive resulting events. The instance will be closed", ex))
				.onErrorResume(ex -> Mono.just(new OnUpdateError(userId, new TdApi.Error(500, ex.getMessage()))))

				// when an error arrives, close the session
				.take(1, true)
				.concatMap(ignored -> Mono
						.from(rawTelegramClient.send(new TdApi.Close(), SPECIAL_RAW_TIMEOUT_DURATION))
						.then(Mono.empty())
				)
				.subscribeOn(Schedulers.parallel())
				.subscribe();

		var messagesToSend = publishedResultingEvents
				// Obtain only client-bound events
				.filter(s -> s instanceof ClientBoundResultingEvent)
				.cast(ClientBoundResultingEvent.class)
				.map(ClientBoundResultingEvent::event)

				// Buffer requests to avoid halting the event loop
				.onBackpressureBuffer();

		kafkaTdlibServer.events().sendMessages(userId, messagesToSend).subscribeOn(Schedulers.parallel()).subscribe();

		publishedResultingEvents
				// Obtain only cluster-bound events
				.filter(s -> s instanceof ClusterBoundResultingEvent)
				.cast(ClusterBoundResultingEvent.class)

				// Buffer requests to avoid halting the event loop
				.onBackpressureBuffer()

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

	private <T extends TdApi.Object> Mono<TdApi.Object> fixBrokenKey(Function<T> function, TdApi.Object result) {
		if (result.getConstructor() == TdApi.Error.CONSTRUCTOR
				&& function instanceof TdApi.CheckDatabaseEncryptionKey checkDatabaseEncryptionKey) {
			// Fix legacy "cucumbers" password
			if (checkDatabaseEncryptionKey.encryptionKey == null
					&& "Wrong password".equals(((TdApi.Error) result).message)) {

				var checkOldKeyFunction = new TdApi.CheckDatabaseEncryptionKey("cucumber".getBytes(StandardCharsets.US_ASCII));
				Mono<TdApi.Object> oldKeyCheckResultMono = Mono
						.from(rawTelegramClient.send(checkOldKeyFunction, SPECIAL_RAW_TIMEOUT_DURATION));
				return oldKeyCheckResultMono.flatMap(oldKeyCheckResult -> {
					if (oldKeyCheckResult.getConstructor() != TdApi.Error.CONSTRUCTOR) {
						var fixOldKeyFunction = new TdApi.SetDatabaseEncryptionKey();
						return Mono
								.from(rawTelegramClient.send(fixOldKeyFunction, SPECIAL_RAW_TIMEOUT_DURATION));
					} else {
						return Mono.just(oldKeyCheckResult);
					}
				});
			}
		}
		return Mono.just(result);
	}

	protected abstract boolean isBot();

	private ResultingEvent wrapUpdateSignal(Signal signal) {
		var update = (TdApi.Update) signal.getUpdate();
		return new ClientBoundResultingEvent(new OnUpdateData(userId, update));
	}

	private List<ResultingEvent> withUpdateSignal(Signal signal, List<ResultingEvent> list) {
		var result = new ArrayList<ResultingEvent>(list.size() + 1);
		result.add(wrapUpdateSignal(signal));
		result.addAll(list);
		return result;
	}

	@NotNull
	private List<@NotNull ResultingEvent> onSignal(Signal signal) {
		// Update the state
		var state = this.state.updateAndGet(oldState -> oldState.withSignal(signal));

		if (state.authPhase() == LOGGED_IN) {
			ResultingEvent resultingEvent = wrapUpdateSignal(signal);
			return List.of(resultingEvent);
		} else {
			LOG.trace("Signal has not been broadcast because the session {} is not logged in: {}", userId, signal);
			return this.handleSpecialSignal(state, signal);
		}
	}

	@SuppressWarnings("SwitchStatementWithTooFewBranches")
	@NotNull
	private List<@NotNull ResultingEvent> handleSpecialSignal(State state, Signal signal) {
		if (signal.isException()) {
			LOG.error("Received an error signal", signal.getException());
			return List.of();
		}
		if (signal.isClosed()) {
			signal.getClosed();
			LOG.info("Received a closed signal");
			return List.of(new ClientBoundResultingEvent(new OnUpdateData(userId,
					new TdApi.UpdateAuthorizationState(new AuthorizationStateClosed())
			)), new ResultingEventPublisherClosed());
		}
		if (signal.isUpdate() && signal.getUpdate().getConstructor() == TdApi.Error.CONSTRUCTOR) {
			var error = ((TdApi.Error) signal.getUpdate());
			LOG.error("Received a TDLib error signal! Error {}: {}", error.code, error.message);
			return List.of();
		}
		if (!signal.isUpdate()) {
			LOG.error("Received a signal that's not an update: {}", signal);
			return List.of();
		}
		var update = signal.getUpdate();
		var updateResult = wrapUpdateSignal(signal);
		switch (state.authPhase()) {
			case BROKEN -> {}
			case PARAMETERS_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR -> {
								TdlibParameters parameters = generateTDLibParameters();
								return List.of(updateResult, new TDLibBoundResultingEvent<>(new SetTdlibParameters(parameters)));
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
								return List.of(updateResult, new TDLibBoundResultingEvent<>(new CheckDatabaseEncryptionKey()));
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
								return withUpdateSignal(signal, onWaitCode());
							}
							case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR -> {
								var link = ((AuthorizationStateWaitOtherDeviceConfirmation) updateAuthorizationState.authorizationState).link;
								return List.of(updateResult,
										new ClientBoundResultingEvent(new OnOtherDeviceLoginRequested(userId, link)));
							}
							case TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR -> {
								var authorizationStateWaitPassword = ((AuthorizationStateWaitPassword) updateAuthorizationState.authorizationState);
								return List.of(updateResult,
										new ClientBoundResultingEvent(new OnPasswordRequested(userId,
												authorizationStateWaitPassword.passwordHint,
												authorizationStateWaitPassword.hasRecoveryEmailAddress,
												authorizationStateWaitPassword.recoveryEmailAddressPattern
										))
								);
							}
							case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR -> {
								return withUpdateSignal(signal, onWaitToken());
							}
						}
					}
				}
			}
		}
		return List.of();
	}

	private TdlibParameters generateTDLibParameters() {
		var tdlibParameters = new TdlibParameters();
		var path = requireNonNull(this.path.get(), "Path must not be null");
		tdlibParameters.databaseDirectory = path + "?use_custom_database_format=true";
		tdlibParameters.apiId = 376588;
		tdlibParameters.apiHash = "2143fdfc2bbba3ec723228d2f81336c9";
		tdlibParameters.filesDirectory = path.resolve("user_storage").toString();
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

	protected abstract List<ResultingEvent> onWaitToken();

	protected List<ResultingEvent> onWaitCode() {
		LOG.error("Wait code event is not supported");
		return List.of();
	}

	public static byte[] serializeEvents(List<ClientBoundEvent> clientBoundEvents) {
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				dataOutputStream.writeInt(clientBoundEvents.size());
				for (ClientBoundEvent clientBoundEvent : clientBoundEvents) {
					writeClientBoundEvent(clientBoundEvent, dataOutputStream);
				}
				return byteArrayOutputStream.toByteArray();
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	public static byte[] serializeEvent(ClientBoundEvent clientBoundEvent) {
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				writeClientBoundEvent(clientBoundEvent, dataOutputStream);
				return byteArrayOutputStream.toByteArray();
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	private static void writeClientBoundEvent(ClientBoundEvent clientBoundEvent, DataOutputStream dataOutputStream)
			throws IOException {
		dataOutputStream.writeLong(clientBoundEvent.userId());
		dataOutputStream.writeInt(SERIAL_VERSION);
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
		} else if (clientBoundEvent instanceof OnOtherDeviceLoginRequested onOtherDeviceLoginRequested) {
			dataOutputStream.writeByte(0x5);
			dataOutputStream.writeUTF(onOtherDeviceLoginRequested.link());
		} else if (clientBoundEvent instanceof OnPasswordRequested onPasswordRequested) {
			dataOutputStream.writeByte(0x6);
			dataOutputStream.writeUTF(onPasswordRequested.passwordHint());
			dataOutputStream.writeBoolean(onPasswordRequested.hasRecoveryEmail());
			dataOutputStream.writeUTF(onPasswordRequested.recoveryEmailPattern());
		} else {
			throw new UnsupportedOperationException("Unexpected value: " + clientBoundEvent);
		}
	}

	@SuppressWarnings("unchecked")
	private Disposable registerTopics() {
		var subscription1 = kafkaTdlibServer.request().consumeMessages("td-requests-handler", userId)
				.flatMapSequential(req -> this
						.handleRequest(req.data())
						.doOnNext(response -> this.responses.emitNext(response, EmitFailureHandler.busyLooping(TEN_MS)))
						.then()
				)
				.subscribeOn(Schedulers.parallel())
				.subscribe();
		var subscription2 = this.kafkaTdlibServer
				.response()
				.sendMessages(userId, responses.asFlux())
				.subscribeOn(Schedulers.parallel())
				.subscribe();
		return () -> {
			subscription1.dispose();
			subscription2.dispose();
		};
	}

	private static byte[] serializeResponse(Response response) {
		if (response == null) return null;
		var id = response.getId();
		var object = response.getObject();
		try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (var dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				dataOutputStream.writeInt(SERIAL_VERSION);
				//dataOutputStream.writeLong(id);
				object.serialize(dataOutputStream);
				return byteArrayOutputStream.toByteArray();
			}
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	private Mono<Event.OnResponse.Response<TdApi.Object>> handleRequest(OnRequest<TdApi.Object> onRequestObj) {
		if (onRequestObj instanceof OnRequest.InvalidRequest invalidRequest) {
			return Mono.just(new Event.OnResponse.Response<>(invalidRequest.clientId(),
					invalidRequest.requestId(),
					userId,
					new TdApi.Error(400, "Conflicting protocol version")
			));
		}
		var requestObj = (Request<Object>) onRequestObj;
		var requestWithTimeoutInstant = new RequestWithTimeoutInstant<>(requestObj.request(), requestObj.timeout());
		var state = this.state.get();
		if (state.authPhase() == LOGGED_IN) {
			var request = requestWithTimeoutInstant.request();
			var timeoutDuration = Duration.between(Instant.now(), requestWithTimeoutInstant.timeout());
			if (timeoutDuration.isZero() || timeoutDuration.isNegative()) {
				LOG.warn("Received an expired request. Expiration: {}", requestWithTimeoutInstant.timeout());
			}

			return Mono
					.from(rawTelegramClient.send(request, timeoutDuration))
					.map(responseObj -> new Event.OnResponse.Response<>(onRequestObj.clientId(),
							onRequestObj.requestId(),
							userId, responseObj))
					.publishOn(Schedulers.parallel());
		} else {
			LOG.error("Ignored a request because the current state is {}. Request: {}", state, requestObj);
			return Mono.just(new Event.OnResponse.Response<>(onRequestObj.clientId(),
					onRequestObj.requestId(),
					userId, new TdApi.Error(503, "Service Unavailable: " + state)));
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ReactiveApiPublisher.class.getSimpleName() + "[", "]")
				.add("userId=" + userId)
				.toString();
	}

	private record RequestWithTimeoutInstant<T extends TdApi.Object>(TdApi.Function<T> request, Instant timeout) {}

	private static class ReactiveApiPublisherToken extends ReactiveApiPublisher {

		private final String botToken;

		public ReactiveApiPublisherToken(KafkaTdlibServer kafkaTdlibServer,
				Set<ResultingEventTransformer> resultingEventTransformerSet,
				long userId,
				String botToken) {
			super(kafkaTdlibServer, resultingEventTransformerSet, userId);
			this.botToken = botToken;
		}

		@Override
		protected boolean isBot() {
			return true;
		}

		@Override
		protected List<ResultingEvent> onWaitToken() {
			return List.of(new TDLibBoundResultingEvent<>(new CheckAuthenticationBotToken(botToken)));
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", ReactiveApiPublisherToken.class.getSimpleName() + "[", "]")
					.add("userId=" + userId)
					.add("token='" + botToken + "'")
					.toString();
		}
	}

	private static class ReactiveApiPublisherPhoneNumber extends ReactiveApiPublisher {

		private final long phoneNumber;

		public ReactiveApiPublisherPhoneNumber(KafkaTdlibServer kafkaTdlibServer,
				Set<ResultingEventTransformer> resultingEventTransformerSet,
				long userId,
				long phoneNumber) {
			super(kafkaTdlibServer, resultingEventTransformerSet, userId);
			this.phoneNumber = phoneNumber;
		}

		@Override
		protected boolean isBot() {
			return false;
		}

		@Override
		protected List<ResultingEvent> onWaitToken() {
			var authSettings = new PhoneNumberAuthenticationSettings();
			authSettings.allowFlashCall = false;
			authSettings.allowSmsRetrieverApi = false;
			authSettings.isCurrentPhoneNumber = false;
			return List.of(new TDLibBoundResultingEvent<>(new SetAuthenticationPhoneNumber("+" + phoneNumber,
					authSettings
			)));
		}

		@Override
		public List<ResultingEvent> onWaitCode() {
			return List.of(new ClientBoundResultingEvent(new OnUserLoginCodeRequested(userId, phoneNumber)));
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", ReactiveApiPublisherPhoneNumber.class.getSimpleName() + "[", "]")
					.add("userId=" + userId)
					.add("phoneNumber=" + phoneNumber)
					.toString();
		}
	}
}
