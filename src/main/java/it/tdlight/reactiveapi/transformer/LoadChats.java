package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.ChatListArchive;
import it.tdlight.jni.TdApi.ChatListMain;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import java.util.List;
import reactor.core.publisher.Flux;

public class LoadChats implements ResultingEventTransformer {

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		if (isBot) {
			return events;
		}
		return events.concatMapIterable(event -> {

			// Append the LoadChat call if the ready auth state is intercepted
			if (event instanceof ClientBoundResultingEvent clientBoundResultingEvent
					&& clientBoundResultingEvent.event() instanceof OnUpdateData onUpdate
					&& onUpdate.update() instanceof TdApi.UpdateAuthorizationState authorizationState
					&& authorizationState.authorizationState instanceof TdApi.AuthorizationStateReady) {
				return List.of(event,
						new TDLibBoundResultingEvent<>(new TdApi.LoadChats(new ChatListMain(), 500), true),
						new TDLibBoundResultingEvent<>(new TdApi.LoadChats(new ChatListArchive(), 500), true)
				);
			}

			return List.of(event);
		});
	}
}
