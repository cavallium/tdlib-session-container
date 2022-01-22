package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.OptionValueBoolean;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import java.util.List;
import reactor.core.publisher.Flux;

public class DisableLogs implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.concatMapIterable(event -> {

			// Append SetVerbosityLevel after SetTDLibParameters
			if (event instanceof TDLibBoundResultingEvent tdLibBoundResultingEvent
					&& tdLibBoundResultingEvent.action() instanceof TdApi.SetTdlibParameters) {
				return List.of(event, new TDLibBoundResultingEvent<>(new TdApi.SetLogVerbosityLevel(0)));
			}

			return List.of(event);
		});
	}
}
