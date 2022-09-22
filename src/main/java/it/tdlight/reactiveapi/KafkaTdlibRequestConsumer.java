package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.reactiveapi.Event.OnRequest;

public class KafkaTdlibRequestConsumer extends KafkaConsumer<OnRequest<TdApi.Object>> {

	public KafkaTdlibRequestConsumer(KafkaParameters kafkaParameters) {
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

	@Override
	public boolean isQuickResponse() {
		return true;
	}

}
