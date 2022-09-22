package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public class KafkaClientBoundConsumer extends KafkaConsumer<ClientBoundEvent> {

	private static final KafkaChannelCodec CODEC = KafkaChannelCodec.CLIENT_BOUND_EVENT;
	private final String lane;
	private final String name;

	public KafkaClientBoundConsumer(KafkaParameters kafkaParameters, String lane) {
		super(kafkaParameters);
		this.lane = lane;
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

	@Override
	public boolean isQuickResponse() {
		return false;
	}

}
