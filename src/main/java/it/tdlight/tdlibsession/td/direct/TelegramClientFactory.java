package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.json.JsonObject;
import it.tdlight.common.TelegramClient;
import it.tdlight.tdlight.ClientManager;
import it.tdlight.utils.MonoUtils;
import reactor.core.publisher.Mono;

public class TelegramClientFactory {

	public TelegramClientFactory() {

	}

	public Mono<TelegramClient> create(JsonObject implementationDetails) {
		return MonoUtils.fromBlockingSingle(() -> {
			var implementationName = implementationDetails.getString("name", "native-client");
			switch (implementationName) {
				case "native-client":
					return ClientManager.create();
				case "test-client":
					//todo: create a noop test client with optional behaviours
				default:
					return null;
			}
		});
	}
}
