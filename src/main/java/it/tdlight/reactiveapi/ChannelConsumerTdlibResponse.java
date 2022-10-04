package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnResponse;

public class ChannelConsumerTdlibResponse {

	private ChannelConsumerTdlibResponse() {
	}

	public static EventConsumer<OnResponse<Object>> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters) {
		return channelFactory.newConsumer(kafkaParameters,
				true,
				ChannelCodec.TDLIB_RESPONSE,
				ChannelCodec.TDLIB_RESPONSE.getKafkaName()
		);
	}
}
