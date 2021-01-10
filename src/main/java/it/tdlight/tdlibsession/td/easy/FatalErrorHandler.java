package it.tdlight.tdlibsession.td.easy;

import it.tdlight.tdlibsession.FatalErrorType;
import reactor.core.publisher.Mono;

public interface FatalErrorHandler {
	Mono<Void> onFatalError(FatalErrorType error);
}
