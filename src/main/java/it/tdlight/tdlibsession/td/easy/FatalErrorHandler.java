package it.tdlight.tdlibsession.td.easy;

import it.tdlight.tdlibsession.FatalErrorType;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public interface FatalErrorHandler {
	@NotNull Mono<Void> onFatalError(FatalErrorType error);
}
