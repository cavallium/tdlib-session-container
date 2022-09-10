package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class KafkaClientBoundConsumer extends KafkaConsumer<ClientBoundEvent> {

	public KafkaClientBoundConsumer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.CLIENT_BOUND_EVENT;
	}

	@Override
	public boolean isQuickResponse() {
		return false;
	}

}
