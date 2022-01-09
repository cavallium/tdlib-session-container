package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi.CheckAuthenticationCode;
import it.tdlight.jni.TdApi.CheckAuthenticationPassword;
import it.tdlight.reactiveapi.Cli;
import it.tdlight.reactiveapi.Event.OnBotLoginCodeRequested;
import it.tdlight.reactiveapi.Event.OnOtherDeviceLoginRequested;
import it.tdlight.reactiveapi.Event.OnPasswordRequested;
import it.tdlight.reactiveapi.Event.OnUserLoginCodeRequested;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CommandLineAuth implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.flatMapSequential(event -> {
			if (event instanceof ClientBoundResultingEvent clientBoundResultingEvent) {
				if (clientBoundResultingEvent.event() instanceof OnUserLoginCodeRequested requested) {
					return this
							.askParam("Please type the login code of " + requested.phoneNumber(), requested.userId())
							.<ResultingEvent>map(code -> new TDLibBoundResultingEvent<>(new CheckAuthenticationCode(code)));
				} else if (clientBoundResultingEvent.event() instanceof OnBotLoginCodeRequested requested) {
					return this
							.askParam("Please type the login code of " + requested.token(), requested.userId())
							.<ResultingEvent>map(code -> new TDLibBoundResultingEvent<>(new CheckAuthenticationCode(code)));
				} else if (clientBoundResultingEvent.event() instanceof OnPasswordRequested onPasswordRequested) {
					return this
							.askParam("Please type the password. Hint: " + onPasswordRequested.passwordHint() + (
									onPasswordRequested.hasRecoveryEmail() ? " Recovery e-mail: "
											+ onPasswordRequested.recoveryEmailPattern() : ""), onPasswordRequested.userId())
							.<ResultingEvent>map(password -> new TDLibBoundResultingEvent<>(new CheckAuthenticationPassword(password)));
				} else if (clientBoundResultingEvent.event() instanceof OnOtherDeviceLoginRequested onOtherDeviceLoginRequested) {
					return this
							.askParam("Please confirm the login on another other device, (after copying the link press enter): "
									+ onOtherDeviceLoginRequested.link(), onOtherDeviceLoginRequested.userId())
							.then(Mono.empty());
				}
			}
			return Mono.just(event);
		});
	}

	private Mono<String> askParam(String text, long userId) {
		return Mono
				.fromCallable(() -> Cli.askParameter("[#IDU" + userId + "]" + text))
				.subscribeOn(Schedulers.boundedElastic());
	}
}
