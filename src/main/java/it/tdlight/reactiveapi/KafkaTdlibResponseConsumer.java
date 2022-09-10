package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnResponse;

public class KafkaTdlibResponseConsumer extends KafkaConsumer<OnResponse<TdApi.Object>> {

	public KafkaTdlibResponseConsumer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.TDLIB_RESPONSE;
	}

	@Override
	public boolean isQuickResponse() {
		return true;
	}

}
