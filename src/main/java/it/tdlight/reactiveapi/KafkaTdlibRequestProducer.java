package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.OnRequest;

public class KafkaTdlibRequestProducer extends KafkaProducer<OnRequest<?>> {

	public KafkaTdlibRequestProducer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.TDLIB_REQUEST;
	}
}
