package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class KafkaClientBoundConsumer extends KafkaConsumer<ClientBoundEvent> {

	private final String lane;
	private final String name;

	public KafkaClientBoundConsumer(KafkaParameters kafkaParameters, String lane) {
		super(kafkaParameters);
		this.lane = lane;
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

	@Override
	public boolean isQuickResponse() {
		return false;
	}

}
