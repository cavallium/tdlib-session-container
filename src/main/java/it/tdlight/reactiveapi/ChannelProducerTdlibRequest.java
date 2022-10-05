package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.OnRequest;

public class ChannelProducerTdlibRequest {

	private ChannelProducerTdlibRequest() {
	}

	public static EventProducer<OnRequest<?>> create(ChannelFactory channelFactory, ChannelsParameters channelsParameters) {
		return channelFactory.newProducer(channelsParameters,
				ChannelCodec.TDLIB_REQUEST,
				Channel.TDLIB_REQUEST.getChannelName()
		);
	}

}
