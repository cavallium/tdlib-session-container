package it.tdlight.tdlibsession.td.direct;

import io.vertx.core.AsyncResult;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.tdlibsession.td.TdResult;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AsyncTdDirect {

	/**
	 * Receives incoming updates and request responses from TDLib. May be called from any thread, but
	 * shouldn't be called simultaneously from two different threads.
	 *
	 * @param receiveDuration Maximum number of seconds allowed for this function to wait for new records. Default: 1 sec
	 * @param eventsSize Maximum number of events allowed in list. Default: 350 events
	 * @return An incoming update or request response list. The object returned in the response may be
	 * an empty list if the timeout expires.
	 */
	Flux<AsyncResult<TdResult<Update>>> getUpdates(Duration receiveDuration, int eventsSize);

	/**
	 * Sends request to TDLib. May be called from any thread.
	 *
	 * @param request Request to TDLib.
	 * @param synchronous Execute synchronously.
	 * @return The request response or {@link it.tdlight.jni.TdApi.Error}.
	 */
	<T extends TdApi.Object> Mono<TdResult<T>> execute(Function request, boolean synchronous);

	/**
	 * Initializes the client and TDLib instance.
	 */
	Mono<Void> initializeClient();

	/**
	 * Destroys the client and TDLib instance.
	 */
	Mono<Void> destroyClient();
}
