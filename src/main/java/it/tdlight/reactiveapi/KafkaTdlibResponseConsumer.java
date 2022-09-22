package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnResponse;

public class KafkaTdlibResponseConsumer extends KafkaConsumer<OnResponse<TdApi.Object>> {

	public KafkaTdlibResponseConsumer(KafkaParameters kafkaParameters) {
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

	@Override
	public boolean isQuickResponse() {
		return true;
	}

}
