package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnResponse;

public class ChannelConsumerTdlibResponse {

	private ChannelConsumerTdlibResponse() {
	}

	public static EventConsumer<OnResponse<Object>> create(ChannelFactory channelFactory) {
		return channelFactory.newConsumer(true, ChannelCodec.TDLIB_RESPONSE, Channel.TDLIB_RESPONSE.getChannelName());
	}
}
