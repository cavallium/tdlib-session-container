package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorTelegramClient {

	Flux<Object> initialize();

	Mono<Object> send(TdApi.Function query);

	Object execute(TdApi.Function query);
}
