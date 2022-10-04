package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class ChannelProducerClientBoundEvent {

	private static final ChannelCodec CODEC = ChannelCodec.CLIENT_BOUND_EVENT;

	private ChannelProducerClientBoundEvent() {
	}

	public static EventProducer<ClientBoundEvent> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters, String lane) {
		String name;
		if (lane.isBlank()) {
			name = CODEC.getKafkaName();
		} else {
			name = CODEC.getKafkaName() + "-" + lane;
		}
		return channelFactory.newProducer(kafkaParameters,
				CODEC,
				name
		);
	}
}
