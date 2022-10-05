package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class ChannelProducerClientBoundEvent {

	private ChannelProducerClientBoundEvent() {
	}

	public static EventProducer<ClientBoundEvent> create(ChannelFactory channelFactory, ChannelsParameters channelsParameters, String lane) {
		String name;
		if (lane.isBlank()) {
			name = Channel.CLIENT_BOUND_EVENT.getChannelName();
		} else {
			name = Channel.CLIENT_BOUND_EVENT.getChannelName() + "-" + lane;
		}
		return channelFactory.newProducer(channelsParameters, ChannelCodec.CLIENT_BOUND_EVENT, name);
	}
}
