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

public class DefaultOptions implements ResultingEventTransformer {

	private static final Collection<ResultingEvent> DEFAULT_OPTIONS = List.of(
			setInt("message_unload_delay", 1800),
			setBoolean("disable_persistent_network_statistics", true),
			setBoolean("disable_time_adjustment_protection", true),
			setBoolean("ignore_inline_thumbnails", true),
			setBoolean("ignore_platform_restrictions", true),
			setBoolean("use_storage_optimizer", true)
	);

	private static final Collection<ResultingEvent> DEFAULT_USER_OPTIONS = List.of(
			setBoolean("disable_animated_emoji", true),
			setBoolean("disable_contact_registered_notifications", true),
			setBoolean("disable_top_chats", true),
			setInt("notification_group_count_max", 0),
			setInt("notification_group_size_max", 1)
	);

	@Override
	public Flux<ResultingEvent> transform(boolean isBot, Flux<ResultingEvent> events) {
		return events.concatMapIterable(event -> {

			// Append the options if the initial auth state is intercepted
			if (event instanceof ClientBoundResultingEvent clientBoundResultingEvent
					&& clientBoundResultingEvent.event() instanceof OnUpdateData onUpdate
					&& onUpdate.update() instanceof TdApi.UpdateAuthorizationState authorizationState
					&& authorizationState.authorizationState instanceof TdApi.AuthorizationStateWaitTdlibParameters) {

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
