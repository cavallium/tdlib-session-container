package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.AuthorizationStateClosing;
import it.tdlight.jni.TdApi.AuthorizationStateReady;
import it.tdlight.jni.TdApi.Close;
import it.tdlight.jni.TdApi.ConnectionStateReady;
import it.tdlight.jni.TdApi.FormattedText;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.GetMe;
import it.tdlight.jni.TdApi.Message;
import it.tdlight.jni.TdApi.MessageSenderUser;
import it.tdlight.jni.TdApi.MessageText;
import it.tdlight.jni.TdApi.Ok;
import it.tdlight.jni.TdApi.SetLogTagVerbosityLevel;
import it.tdlight.jni.TdApi.SetLogVerbosityLevel;
import it.tdlight.jni.TdApi.SetOption;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.TextEntity;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.jni.TdApi.UpdateConnectionState;
import it.tdlight.jni.TdApi.UpdateNewMessage;
import it.tdlight.jni.TdApi.User;
import it.tdlight.tdlibsession.td.ReactorTelegramClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;

public class TestClient implements ReactorTelegramClient {

	private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

	private static final AtomicLong incrementalMessageId = new AtomicLong(1);
	private final List<String> features;
	private final Empty<java.lang.Object> closedSink = Sinks.empty();

	public TestClient(JsonObject testClientSettings) {
		JsonArray features = testClientSettings.getJsonArray("features", new JsonArray());
		this.features = new ArrayList<>();
		for (java.lang.Object feature : features) {
			var featureName = (String) feature;
			this.features.add(featureName);
		}
	}

	private static Message generateRandomMessage(boolean randomSender, boolean randomChat, boolean randomText) {
		var msg = new Message();
		msg.senderId = new MessageSenderUser(312042);
		msg.chatId = 240213;
		msg.id = incrementalMessageId.getAndIncrement();
		var content = new MessageText();
		content.text = new FormattedText("Text", new TextEntity[0]);
		msg.content = content;
		msg.date = (int) System.currentTimeMillis() / 1000;
		return msg;
	}

	@Override
	public Mono<Void> initialize() {
		return Mono.empty();
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return Flux.fromIterable(features).flatMap(featureName -> {
			switch (featureName) {
				case "status-update":
					return Flux
							.<TdApi.Object>just(
									new UpdateAuthorizationState(new AuthorizationStateReady()),
									new UpdateConnectionState(new ConnectionStateReady())
							)
							.mergeWith(closedSink
									.asMono()
									.thenMany(Flux.just(new UpdateAuthorizationState(new AuthorizationStateClosing()),
											new UpdateAuthorizationState(new AuthorizationStateClosed())
									))
							);
				case "infinite-messages":
					var randomSenders = features.contains("random-senders");
					var randomChats = features.contains("random-chats");
					var randomTexts = features.contains("random-text");
					return Flux
							.<TdApi.Object>fromIterable(() -> new Iterator<>() {
								@Override
								public boolean hasNext() {
									return true;
								}

								@Override
								public TdApi.Object next() {
									return new UpdateNewMessage(generateRandomMessage(randomSenders, randomChats, randomTexts));
								}
							}).takeUntilOther(this.closedSink.asMono());
				default:
					return Mono.fromCallable(() -> {
						throw new IllegalArgumentException("Unknown feature name: " + featureName);
					});
			}
		});
	}

	@Override
	public <T extends TdApi.Object> Mono<TdApi.Object> send(Function<T> query, Duration timeout) {
		return Mono.fromCallable(() -> {
			TdApi.Object result = executeCommon(query);
			if (result != null) {
				return result;
			}
			return new TdApi.Error(500, "Unsupported");
		});
	}

	@Override
	public <T extends TdApi.Object> TdApi.Object execute(Function<T> query) {
		TdApi.Object result = executeCommon(query);
		if (result != null) {
			return result;
		}
		return new TdApi.Error(500, "Unsupported");
	}

	@Nullable
	public <T extends TdApi.Object> TdApi.Object executeCommon(Function<T> query) {
		switch (query.getConstructor()) {
			case SetLogVerbosityLevel.CONSTRUCTOR:
			case SetLogTagVerbosityLevel.CONSTRUCTOR:
			case SetTdlibParameters.CONSTRUCTOR:
			case SetOption.CONSTRUCTOR:
				return new Ok();
			case GetMe.CONSTRUCTOR:
				var user = new User();
				user.id = 420;
				user.firstName = "Test";
				user.lastName = "Test";
				user.phoneNumber = "+77";
				return user;
			case Close.CONSTRUCTOR:
				closedSink.tryEmitEmpty();
				return new Ok();
		}
		return null;
	}
}
