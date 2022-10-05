package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import org.jetbrains.annotations.NotNull;

public class ChannelConsumerClientBoundEvent {

	private ChannelConsumerClientBoundEvent() {
	}

	public static EventConsumer<ClientBoundEvent> create(ChannelFactory channelFactory, ChannelsParameters channelsParameters,
			@NotNull String lane) {
		String name;
		if (lane.isEmpty()) {
			name = Channel.CLIENT_BOUND_EVENT.getChannelName();
		} else {
			name = Channel.CLIENT_BOUND_EVENT.getChannelName() + "-" + lane;
		}
		return channelFactory.newConsumer(channelsParameters, false, ChannelCodec.CLIENT_BOUND_EVENT, name);
	}

}
