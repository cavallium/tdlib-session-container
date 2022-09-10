package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.ServerBoundEvent;

public class KafkaTdlibRequestConsumer extends KafkaConsumer<OnRequest<TdApi.Object>> {

	public KafkaTdlibRequestConsumer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.TDLIB_REQUEST;
	}

	@Override
	public boolean isQuickResponse() {
		return true;
	}

}
