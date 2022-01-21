package it.tdlight.reactiveapi.transformer;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.OptionValueBoolean;
import it.tdlight.jni.TdApi.OptionValueInteger;
import it.tdlight.reactiveapi.Event.OnUpdateData;
import it.tdlight.reactiveapi.ResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEventTransformer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import reactor.core.publisher.Flux;

public class TdlightDefaultOptions implements ResultingEventTransformer {

	private static final Collection<ResultingEvent> DEFAULT_OPTIONS = List.of(
			setBoolean("disable_document_filenames", true),
			setBoolean("disable_minithumbnails", true),
			setBoolean("disable_notifications", true),
			setBoolean("ignore_update_chat_last_message", true),
			setBoolean("ignore_update_chat_read_inbox", true),
			setBoolean("ignore_update_user_chat_action", true),
			setBoolean("ignore_server_deletes_and_reads", true)
	);

	private static final Collection<ResultingEvent> DEFAULT_USER_OPTIONS = List.of();

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.concatMapIterable(event -> {

			// Append the options if the initial auth state is intercepted
			if (event instanceof ClientBoundResultingEvent clientBoundResultingEvent
					&& clientBoundResultingEvent.event() instanceof OnUpdateData onUpdate
					&& onUpdate.update() instanceof TdApi.UpdateAuthorizationState authorizationState
					&& authorizationState.authorizationState instanceof TdApi.AuthorizationStateWaitEncryptionKey) {

				var resultingEvent = new ArrayList<ResultingEvent>(1 + DEFAULT_OPTIONS.size() + DEFAULT_USER_OPTIONS.size());
				// Add the intercepted event
				resultingEvent.add(event);
				// Add the default options
				resultingEvent.addAll(DEFAULT_OPTIONS);
				// Add user-only default options
				if (!isBot) {
					resultingEvent.addAll(DEFAULT_USER_OPTIONS);
				}
				return resultingEvent;
			} else {
				// Return just the intercepted event as-is
				return List.of(event);
			}
		});
	}

	private static ResultingEvent setBoolean(String optionName, boolean value) {
		return new TDLibBoundResultingEvent<>(new TdApi.SetOption(optionName, new OptionValueBoolean(value)));
	}

	private static ResultingEvent setInt(String optionName, int value) {
		return new TDLibBoundResultingEvent<>(new TdApi.SetOption(optionName, new OptionValueInteger(value)));
	}
}
