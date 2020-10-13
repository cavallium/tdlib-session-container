package it.tdlight.tdlibsession.td.middle.server;

import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Mono;

public class RequestId {

	public static Mono<Long> create() {
		AtomicLong _requestId = new AtomicLong(1);

		return Mono.fromCallable(() -> _requestId.updateAndGet(n -> {
			if (n > Long.MAX_VALUE - 100) {
				return 1;
			} else {
				return n + 1;
			}
		}));
	}
}
