package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorTelegramClient {

	Mono<Void> initialize();

	Flux<Object> receive();

	<T extends TdApi.Object> Mono<TdApi.Object> send(TdApi.Function<T> query, Duration timeout);

	<T extends TdApi.Object> TdApi.Object execute(TdApi.Function<T> query);
}
