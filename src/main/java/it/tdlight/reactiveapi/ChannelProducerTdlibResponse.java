package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.OnResponse;

public class ChannelProducerTdlibResponse {

	private ChannelProducerTdlibResponse() {
	}

	public static EventProducer<OnResponse<Object>> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters) {
		return channelFactory.newProducer(kafkaParameters,
				ChannelCodec.TDLIB_RESPONSE,
				ChannelCodec.TDLIB_RESPONSE.getKafkaName()
		);
	}

}
