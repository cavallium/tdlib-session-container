package it.tdlight.tdlibsession.td;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorTelegramClient {

	Mono<Void> initialize();

	Flux<Object> receive();

	Mono<TdApi.Object> send(TdApi.Function query, Duration timeout);

	TdApi.Object execute(TdApi.Function query);
}
