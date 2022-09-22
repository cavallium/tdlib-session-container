package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class KafkaClientBoundProducer extends KafkaProducer<ClientBoundEvent> {

	private final String name;

	public KafkaClientBoundProducer(KafkaParameters kafkaParameters, String lane) {
		super(kafkaParameters);
		if (lane.isBlank()) {
			this.name = KafkaChannelCodec.CLIENT_BOUND_EVENT.getKafkaName();
		} else {
			this.name = KafkaChannelCodec.CLIENT_BOUND_EVENT.getKafkaName() + "-" + lane;
		}
	}

	@Override
	public KafkaChannelCodec getChannelCodec() {
		return KafkaChannelCodec.CLIENT_BOUND_EVENT;
	}

	@Override
	public String getChannelName() {
		return name;
	}
}
