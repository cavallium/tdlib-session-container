package it.tdlight.tdlibsession.td.easy;

import reactor.core.publisher.Mono;

public interface ParameterRequestHandler {
	Mono<String> onParameterRequest(Parameter parameter, ParameterInfo parameterInfo);
}
