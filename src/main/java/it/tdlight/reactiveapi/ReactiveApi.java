package it.tdlight.reactiveapi;

import java.util.Map;
import reactor.core.publisher.Mono;

public interface ReactiveApi {

	Mono<Void> start();

	Mono<CreateSessionResponse> createSession(CreateSessionRequest req);

	Mono<Map<Long, String>> getAllUsers();

	boolean is(String nodeId);

	/**
	 * May return empty
	 */
	Mono<Long> resolveUserLiveId(long userId);

	Mono<Void> close();
}
