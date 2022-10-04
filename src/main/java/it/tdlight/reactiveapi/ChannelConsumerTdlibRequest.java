package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnRequest;

public class ChannelConsumerTdlibRequest {

	private ChannelConsumerTdlibRequest() {
	}

	public static EventConsumer<OnRequest<Object>> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters) {
		return channelFactory.newConsumer(kafkaParameters,
				true,
				ChannelCodec.TDLIB_REQUEST,
				ChannelCodec.TDLIB_REQUEST.getKafkaName()
		);
	}

}
