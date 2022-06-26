package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnResponse;

public class KafkaTdlibResponseProducer extends KafkaProducer<OnResponse<TdApi.Object>> {

	public KafkaTdlibResponseProducer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.TDLIB_RESPONSE;
	}
}
