package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnResponse;

public class ChannelProducerTdlibResponse {

	private ChannelProducerTdlibResponse() {
	}

	public static EventProducer<OnResponse<Object>> create(ChannelFactory channelFactory, ChannelsParameters channelsParameters) {
		return channelFactory.newProducer(channelsParameters,
				ChannelCodec.TDLIB_RESPONSE,
				Channel.TDLIB_RESPONSE.getChannelName()
		);
	}

}
