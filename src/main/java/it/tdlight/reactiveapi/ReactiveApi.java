package it.tdlight.reactiveapi;

import java.util.List;
import java.util.Map;
import java.util.Set;
import reactor.core.publisher.Mono;

public interface ReactiveApi {

	Mono<Void> start();

	Mono<CreateSessionResponse> createSession(CreateSessionRequest req);

	ReactiveApiClient client(String subGroupId, long userId);

	Mono<Void> close();

	void waitForExit();
}
