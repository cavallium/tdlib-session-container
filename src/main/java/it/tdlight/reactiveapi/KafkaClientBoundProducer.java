package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class KafkaClientBoundProducer extends KafkaProducer<ClientBoundEvent> {

	private static final KafkaChannelCodec CODEC = KafkaChannelCodec.CLIENT_BOUND_EVENT;

	private final String name;

	public KafkaClientBoundProducer(KafkaParameters kafkaParameters, String lane) {
		super(kafkaParameters);
		if (lane.isBlank()) {
			this.name = CODEC.getKafkaName();
		} else {
			this.name = CODEC.getKafkaName() + "-" + lane;
		}
	}

	@Override
	public KafkaChannelCodec getChannelCodec() {
		return CODEC;
	}

	@Override
	public String getChannelName() {
		return name;
	}
}
