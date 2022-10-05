package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnRequest;

public class ChannelConsumerTdlibRequest {

	private ChannelConsumerTdlibRequest() {
	}

	public static EventConsumer<OnRequest<Object>> create(ChannelFactory channelFactory, ChannelsParameters channelsParameters) {
		return channelFactory.newConsumer(channelsParameters,
				true,
				ChannelCodec.TDLIB_REQUEST,
				Channel.TDLIB_REQUEST.getChannelName()
		);
	}

}
