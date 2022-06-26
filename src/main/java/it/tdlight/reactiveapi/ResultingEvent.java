package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClientBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ClusterBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;

public sealed interface ResultingEvent permits ClientBoundResultingEvent, TDLibBoundResultingEvent,
		ClusterBoundResultingEvent {

	record ClientBoundResultingEvent(ClientBoundEvent event) implements ResultingEvent {}

	record TDLibBoundResultingEvent<T extends TdApi.Object>(TdApi.Function<T> action, boolean ignoreFailure) implements
			ResultingEvent {

		public TDLibBoundResultingEvent(TdApi.Function<T> action) {
			this(action, false);
		}
	}

	sealed interface ClusterBoundResultingEvent extends ResultingEvent permits ResultingEventPublisherClosed {}

	record ResultingEventPublisherClosed() implements ClusterBoundResultingEvent {}
}
