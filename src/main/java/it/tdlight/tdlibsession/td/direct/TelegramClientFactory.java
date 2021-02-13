package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.json.JsonObject;
import it.tdlight.tdlibsession.td.ReactorTelegramClient;
import it.tdlight.tdlibsession.td.WrappedReactorTelegramClient;
import it.tdlight.tdlight.ClientManager;
import it.tdlight.utils.MonoUtils;
import reactor.core.publisher.Mono;

public class TelegramClientFactory {

	public TelegramClientFactory() {

	}

	public Mono<ReactorTelegramClient> create(JsonObject implementationDetails) {
		return MonoUtils.fromBlockingSingle(() -> {
			var implementationName = implementationDetails.getString("name", "native-client");
			switch (implementationName) {
				case "native-client":
					return new WrappedReactorTelegramClient(ClientManager.createReactive());
				case "test-client":
					return new TestClient(implementationDetails.getJsonObject("test-client-settings"));
				default:
					return null;
			}
		});
	}
}
