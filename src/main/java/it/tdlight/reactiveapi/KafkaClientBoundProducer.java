package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;

public class KafkaClientBoundProducer extends KafkaProducer<ClientBoundEvent> {

	public KafkaClientBoundProducer(KafkaParameters kafkaParameters) {
		super(kafkaParameters);
	}

	@Override
	public KafkaChannelName getChannelName() {
		return KafkaChannelName.CLIENT_BOUND_EVENT;
	}
}
