package it.tdlight.reactiveapi;

import java.util.List;
import java.util.Map;
import java.util.Set;
import reactor.core.publisher.Mono;

public interface ReactiveApi {

	Mono<Void> start();

	/**
	 * Send a request to the cluster to load that user id from disk
	 */
	Mono<Void> tryReviveSession(long userId);

	Mono<CreateSessionResponse> createSession(CreateSessionRequest req);

	Mono<Map<Long, String>> getAllUsers();

	Set<UserIdAndLiveId> getLocalLiveSessionIds();

	boolean is(String nodeId);

	/**
	 * May return empty
	 */
	Mono<Long> resolveUserLiveId(long userId);

	ReactiveApiMultiClient multiClient(String subGroupId);

	ReactiveApiClient dynamicClient(String subGroupId, long userId);

	ReactiveApiClient liveClient(String subGroupId, long liveId, long userId);

	Mono<Void> close();
}
