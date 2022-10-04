package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.OnRequest;

public class ChannelProducerTdlibRequest {

	private ChannelProducerTdlibRequest() {
	}

	public static EventProducer<OnRequest<?>> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters) {
		return channelFactory.newProducer(kafkaParameters,
				ChannelCodec.TDLIB_REQUEST,
				ChannelCodec.TDLIB_REQUEST.getKafkaName()
		);
	}

}
