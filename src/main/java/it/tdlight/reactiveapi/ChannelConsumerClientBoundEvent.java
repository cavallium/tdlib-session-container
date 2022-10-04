package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class ChannelConsumerClientBoundEvent {

	private ChannelConsumerClientBoundEvent() {
	}

	public static EventConsumer<ClientBoundEvent> create(ChannelFactory channelFactory, KafkaParameters kafkaParameters,
			String lane) {
		var codec = ChannelCodec.CLIENT_BOUND_EVENT;
		String name;
		if (lane.isBlank()) {
			name = codec.getKafkaName();
		} else {
			name = codec.getKafkaName() + "-" + lane;
		}
		return channelFactory.newConsumer(kafkaParameters, false, codec, name);
	}

}
