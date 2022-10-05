package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnRequest;

public class ChannelConsumerTdlibRequest {

	private ChannelConsumerTdlibRequest() {
	}

	public static EventConsumer<OnRequest<Object>> create(ChannelFactory channelFactory) {
		return channelFactory.newConsumer(true, ChannelCodec.TDLIB_REQUEST, Channel.TDLIB_REQUEST.getChannelName());
	}

}
