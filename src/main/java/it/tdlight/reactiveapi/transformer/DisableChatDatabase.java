package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import java.util.List;
import reactor.core.publisher.Flux;

public class DisableChatDatabase implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.concatMapIterable(event -> {

			// Change option
			if (event instanceof TDLibBoundResultingEvent tdLibBoundResultingEvent
					&& tdLibBoundResultingEvent.action() instanceof TdApi.SetTdlibParameters setTdlibParameters) {
				setTdlibParameters.useChatInfoDatabase = false;
			}

			return List.of(event);
		});
	}
}
