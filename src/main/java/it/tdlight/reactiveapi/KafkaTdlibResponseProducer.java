package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;

public class KafkaTdlibResponseProducer extends KafkaProducer<OnResponse<TdApi.Object>> {

	public KafkaTdlibResponseProducer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelCodec getChannelCodec() {
		return KafkaChannelCodec.TDLIB_RESPONSE;
	}

	@Override
	public String getChannelName() {
		return getChannelCodec().getKafkaName();
	}
}
