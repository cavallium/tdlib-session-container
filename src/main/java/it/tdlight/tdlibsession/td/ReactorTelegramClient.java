package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorTelegramClient {

	Mono<Void> initialize();

	Flux<Object> receive();

	<T extends TdApi.Object> Mono<T> send(TdApi.Function query);

	<T extends TdApi.Object> T execute(TdApi.Function query);
}
