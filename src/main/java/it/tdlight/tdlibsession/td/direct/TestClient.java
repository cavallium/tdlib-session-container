package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import it.tdlight.common.ExceptionHandler;
import it.tdlight.common.ResultHandler;
import it.tdlight.common.TelegramClient;
import it.tdlight.common.UpdatesHandler;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationStateWaitTdlibParameters;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.Ok;
import it.tdlight.jni.TdApi.SetLogTagVerbosityLevel;
import it.tdlight.jni.TdApi.SetLogVerbosityLevel;
import it.tdlight.jni.TdApi.SetOption;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.td.TdError;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TestClient implements TelegramClient {

	private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

	private final Many<Object> updates = Sinks.many().unicast().onBackpressureError();
	private final Scheduler testClientScheduler = Schedulers.newSingle("test-client", true);
	private final List<String> features;
	private UpdatesHandler updatesHandler;
	private ExceptionHandler updateExceptionHandler;
	private ExceptionHandler defaultExceptionHandler;

	public TestClient(JsonObject testClientSettings) {
		JsonArray features = testClientSettings.getJsonArray("features", new JsonArray());
		this.features = new ArrayList<>();
		for (java.lang.Object feature : features) {
			var featureName = (String) feature;
			this.features.add(featureName);
		}
	}

	@Override
	public void initialize(UpdatesHandler updatesHandler,
			ExceptionHandler updateExceptionHandler,
			ExceptionHandler defaultExceptionHandler) {
		this.updatesHandler = updatesHandler;
		this.updateExceptionHandler = updateExceptionHandler;
		this.defaultExceptionHandler = defaultExceptionHandler;

		updates
				.asFlux()
				.buffer(50)
				.doOnNext(ub -> logger.trace("Received update block of size {}", ub.size()))
				.publishOn(testClientScheduler)
				.subscribe(updatesHandler::onUpdates, updateExceptionHandler::onException);

		for (String featureName : features) {
			switch (featureName) {
				case "infinite-status-update":
					Mono.<TdApi.Object>just(new UpdateAuthorizationState(new AuthorizationStateWaitTdlibParameters()))
							.repeat()
							.buffer(100)
							.doOnNext(updatesHandler::onUpdates)
							.publishOn(testClientScheduler)
							.subscribe();
					break;
				default:
					throw new IllegalArgumentException("Unknown feature name: " + featureName);
			}
		}
	}

	@Override
	public void send(Function query, ResultHandler resultHandler, ExceptionHandler exceptionHandler) {
		switch (query.getConstructor()) {
			case SetLogVerbosityLevel.CONSTRUCTOR:
			case SetLogTagVerbosityLevel.CONSTRUCTOR:
			case SetTdlibParameters.CONSTRUCTOR:
			case SetOption.CONSTRUCTOR:
				resultHandler.onResult(new Ok());
				return;
		}
		exceptionHandler.onException(new TdError(500, "Unsupported"));
	}

	@Override
	public Object execute(Function query) {
		switch (query.getConstructor()) {
			case SetLogVerbosityLevel.CONSTRUCTOR:
			case SetLogTagVerbosityLevel.CONSTRUCTOR:
			case SetTdlibParameters.CONSTRUCTOR:
			case SetOption.CONSTRUCTOR:
				return new Ok();
		}
		return new Error(500, "Unsupported");
	}
}
