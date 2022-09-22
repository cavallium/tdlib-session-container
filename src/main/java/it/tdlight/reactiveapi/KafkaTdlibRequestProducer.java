package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.OnRequest;

public class KafkaTdlibRequestProducer extends KafkaProducer<OnRequest<?>> {

	public KafkaTdlibRequestProducer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelCodec getChannelCodec() {
		return KafkaChannelCodec.TDLIB_REQUEST;
	}

	@Override
	public String getChannelName() {
		return getChannelCodec().getKafkaName();
	}
}
