package it.tdlight.tdlibsession.td.middle;

import it.tdlight.jni.TdApi;
import it.tdlight.tdlibsession.td.TdResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AsyncTdMiddle {

	/**
	 * Receives incoming updates from TDLib.
	 *
	 * @return Updates
	 */
	Flux<TdApi.Update> getUpdates();

	/**
	 * Sends request to TDLib. May be called from any thread.
	 *
	 * @param request Request to TDLib.
	 * @param executeDirectly Execute the function synchronously.
	 */
	<T extends TdApi.Object> Mono<TdResult<T>> execute(TdApi.Function request, boolean executeDirectly);
}
