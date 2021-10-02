package it.tdlight.tdlibsession.td.direct;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.tdlibsession.td.TdResult;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AsyncTdDirect {

	Mono<Void> initialize();

	/**
	 * Receives incoming updates and request responses from TDLib.
	 * Can be called only once.
	 *
	 */
	Flux<TdApi.Object> receive(AsyncTdDirectOptions options);

	/**
	 * Sends request to TDLib.
	 * Should be called after receive.
	 *
	 * @param request Request to TDLib.
	 * @param timeout Response timeout.
	 * @param synchronous Execute synchronously.
	 * @return The request response or {@link it.tdlight.jni.TdApi.Error}.
	 */
	<T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, Duration timeout, boolean synchronous);

}
