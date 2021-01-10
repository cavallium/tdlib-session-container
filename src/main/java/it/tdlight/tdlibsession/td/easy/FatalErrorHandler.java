package it.tdlight.tdlibsession.td.easy;

import it.tdlight.tdlibsession.FatalErrorType;
import reactor.core.publisher.Mono;

public interface FatalErrorHandler {
	Mono<String> onFatalError(FatalErrorType error);
}
