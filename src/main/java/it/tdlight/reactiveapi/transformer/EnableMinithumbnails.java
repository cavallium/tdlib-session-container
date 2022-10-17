package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Ok;
import it.tdlight.jni.TdApi.OptionValueBoolean;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import java.util.ArrayList;
import java.util.List;
import reactor.core.publisher.Flux;

public class EnableMinithumbnails implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.concatMapIterable(event -> {

			// Append the options if the initial auth state is intercepted
			if (event instanceof ClientBoundResultingEvent clientBoundResultingEvent
					&& clientBoundResultingEvent.event() instanceof OnUpdateData onUpdate
					&& onUpdate.update() instanceof TdApi.UpdateAuthorizationState authorizationState
					&& authorizationState.authorizationState instanceof TdApi.AuthorizationStateWaitTdlibParameters) {

				var resultingEvent = new ArrayList<ResultingEvent>(2);
				// Add the intercepted event
				resultingEvent.add(event);
				// Enable minithumbnails
				resultingEvent.add(new TDLibBoundResultingEvent<>(new TdApi.SetOption("disable_minithumbnails",
						new OptionValueBoolean(false)), true));
				return resultingEvent;
			} else {
				// Return just the intercepted event as-is
				return List.of(event);
			}
		});
	}

}
