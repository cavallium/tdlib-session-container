package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.AuthPhase.*;

import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.Atomix;
import it.tdlight.common.ReactiveTelegramClient;
import it.tdlight.common.Response;
import it.tdlight.common.Signal;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.CheckAuthenticationBotToken;
import it.tdlight.jni.TdApi.CheckDatabaseEncryptionKey;
import it.tdlight.jni.TdApi.Close;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.PhoneNumberAuthenticationSettings;
import it.tdlight.jni.TdApi.SetAuthenticationPhoneNumber;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.Event.OnUpdateError;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import it.tdlight.reactiveapi.Event.Request;
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
import org.apache.commons.lang3.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ReactiveApiPublisher {


	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApiPublisher.class);
	private static final Duration SPECIAL_RAW_TIMEOUT_DURATION = Duration.ofSeconds(10);

	private final Atomix atomix;
	private final ClusterEventService eventService;
	private final ReactiveTelegramClient rawTelegramClient;
	private final Flux<Signal> telegramClient;
	private final AtomicReference<State> state = new AtomicReference<>(new State(LOGGED_OUT));
	private final long userId;
	private final long liveId;
	private final String botToken;
	private final Long phoneNumber;

	private ReactiveApiPublisher(Atomix atomix, long liveId, long userId, String botToken, Long phoneNumber) {
		this.atomix = atomix;
		this.userId = userId;
		this.liveId = liveId;
		this.botToken = botToken;
		this.phoneNumber = phoneNumber;
		this.rawTelegramClient = ClientManager.createReactive();
		this.telegramClient = Flux.<Signal>create(sink -> {
			rawTelegramClient.setListener(sink::next);
			sink.onCancel(rawTelegramClient::cancel);
			sink.onDispose(rawTelegramClient::dispose);
			rawTelegramClient.createAndRegisterClient();

			this.registerTopics();
		}).share();
		this.eventService = atomix.getEventService();
	}

	public static ReactiveApiPublisher fromToken(Atomix atomix, Long liveId, long userId, String token) {
		return new ReactiveApiPublisher(atomix, liveId, userId, token, null);
	}

	public static ReactiveApiPublisher fromPhoneNumber(Atomix atomix, Long liveId, long userId, long phoneNumber) {
		return new ReactiveApiPublisher(atomix, liveId, userId, null, phoneNumber);
	}

	public void start(Path path) {
		LOG.info("Starting session \"{}\" in path \"{}\"", this, path);
		telegramClient.subscribeOn(Schedulers.parallel()).subscribe(this::onSignal);
	}

	private void onSignal(Signal signal) {
		// Update the state
		var state = this.state.updateAndGet(oldState -> oldState.withSignal(signal));

		if (state.authPhase() == LOGGED_IN) {
			var update = (TdApi.Update) signal.getUpdate();
			var event = new OnUpdateData(liveId, userId, update);
			sendEvent(event);
		} else {
			LOG.trace("Signal has not been broadcasted because the session {} is not logged in: {}", userId, signal);
			this.handleSpecialSignal(state, signal);
		}
	}

	private void handleSpecialSignal(State state, Signal signal) {
		if (signal.isException()) {
			LOG.error("Received an error signal", signal.getException());
			return;
		}
		if (signal.isUpdate() && signal.getUpdate().getConstructor() == TdApi.Error.CONSTRUCTOR) {
			var error = ((TdApi.Error) signal.getUpdate());
			LOG.error("Received a TDLib error signal! Error {}: {}", error.code, error.message);
			return;
		}
		if (!signal.isUpdate()) {
			LOG.error("Received a signal that's not an update: {}", signal);
			return;
		}
		var update = signal.getUpdate();
		switch (state.authPhase()) {
			case BROKEN -> {}
			case PARAMETERS_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR -> sendSpecialRaw(new SetTdlibParameters());
						}
					}
				}
			}
			case ENCRYPTION_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR ->
									sendSpecialRaw(new CheckDatabaseEncryptionKey());
						}
					}
				}
			}
			case AUTH_PHASE -> {
				switch (update.getConstructor()) {
					case TdApi.UpdateAuthorizationState.CONSTRUCTOR -> {
						var updateAuthorizationState = (TdApi.UpdateAuthorizationState) update;
						switch (updateAuthorizationState.authorizationState.getConstructor()) {
							case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR ->
									sendEvent(new OnUserLoginCodeRequested(liveId, userId, phoneNumber));
							case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR ->
									sendEvent(new OnOtherDeviceLoginRequested(liveId, userId));
							case TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR ->
									sendEvent(new OnPasswordRequested(liveId, userId));
							case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR -> {
								if (botToken != null) {
									sendSpecialRaw(new CheckAuthenticationBotToken(botToken));
								} else {
									var authSettings = new PhoneNumberAuthenticationSettings();
									authSettings.allowFlashCall = false;
									authSettings.allowSmsRetrieverApi = false;
									authSettings.isCurrentPhoneNumber = false;
									sendSpecialRaw(new SetAuthenticationPhoneNumber("+" + phoneNumber, authSettings));
								}
							}
						}
					}
				}
			}
		}
	}

	private void sendEvent(ClientBoundEvent clientBoundEvent) {
		eventService.broadcast("session-" + liveId + "-clientbound-events",
				clientBoundEvent,
				ReactiveApiPublisher::serializeEvent
		);
	}

	private void sendSpecialRaw(TdApi.Function<?> function) {
		Mono
				.from(rawTelegramClient.send(function, SPECIAL_RAW_TIMEOUT_DURATION))
				.subscribeOn(Schedulers.parallel())
				.subscribe(resp -> {
					if (resp.getConstructor() == TdApi.Error.CONSTRUCTOR) {
						LOG.error("Received error for special request {}: {}", function, resp);
					}
				}, ex -> LOG.error("Failed to receive the response for special request {}", function, ex));
	}

	private static byte[] serializeEvent(ClientBoundEvent clientBoundEvent) {
		try (var baos = new ByteArrayOutputStream()) {
			try (var daos = new DataOutputStream(baos)) {
				if (clientBoundEvent instanceof OnUpdateData onUpdateData) {
					daos.write(0x1);
					onUpdateData.update().serialize(daos);
				} else if (clientBoundEvent instanceof OnUpdateError onUpdateError) {
					daos.write(0x2);
					onUpdateError.error().serialize(daos);
				} else if (clientBoundEvent instanceof OnUserLoginCodeRequested onUserLoginCodeRequested) {
					daos.write(0x3);
					daos.writeLong(onUserLoginCodeRequested.phoneNumber());
				} else if (clientBoundEvent instanceof OnBotLoginCodeRequested onBotLoginCodeRequested) {
					daos.write(0x4);
					daos.writeUTF(onBotLoginCodeRequested.token());
				} else {
					throw new UnsupportedOperationException("Unexpected value: " + clientBoundEvent);
				}
				return baos.toByteArray();
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
		try (var baos = new ByteArrayOutputStream()) {
			try (var daos = new DataOutputStream(baos)) {
				daos.writeLong(id);
				object.serialize(daos);
				return baos.toByteArray();
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
				.add("botToken='" + botToken + "'")
				.add("phoneNumber=" + phoneNumber)
				.toString();
	}

	private record RequestWithTimeoutInstant<T extends TdApi.Object>(TdApi.Function<T> request, Instant timeout) {}
}
