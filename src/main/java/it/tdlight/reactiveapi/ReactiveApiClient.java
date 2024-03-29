package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Instant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveApiClient extends ReactiveApiThinClient {

	Flux<ClientBoundEvent> clientBoundEvents();

	boolean isPullMode();
}
