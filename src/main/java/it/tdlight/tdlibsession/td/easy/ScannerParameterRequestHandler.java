package it.tdlight.tdlibsession.td.easy;

import it.tdlight.common.utils.ScannerUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ScannerParameterRequestHandler implements ParameterRequestHandler {

	private final String botName;

	public ScannerParameterRequestHandler(String botName) {
		this.botName = botName;
	}

	@Override
	public Mono<String> onParameterRequest(Parameter parameter, ParameterInfo parameterInfo) {
		return Mono.fromCallable(() -> {
			String question;
			boolean trim = false;
			switch (parameter) {
				case ASK_FIRST_NAME: question = "Enter first name"; trim = true; break;
				case ASK_LAST_NAME: question = "Enter last name"; trim = true; break;
				case ASK_CODE: question = "Enter authentication code"; trim = true; break;
				case ASK_PASSWORD:
					question = "Enter your password";
					String passwordMessage = "Password authorization of '" + this.botName + "':";
					String hint = ((ParameterInfoPasswordHint) parameterInfo).getHint();
					if (hint != null && !hint.isBlank()) {
						passwordMessage += "\n\tHint: " + hint;
					}
					System.out.println(passwordMessage);
					break;
				case NOTIFY_LINK:
					System.out.println("Please confirm this login link on another device: "
							+ ((ParameterInfoNotifyLink) parameterInfo).getLink());
					return "";
				default: question = parameter.toString(); break;
			}
			var result = ScannerUtils.askParameter(this.botName, question);
			if (trim) {
				return result.trim();
			} else {
				return result;
			}
		}).publishOn(Schedulers.boundedElastic());
	}
}
