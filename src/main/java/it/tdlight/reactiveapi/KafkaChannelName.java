package it.tdlight.reactiveapi;

import org.apache.kafka.common.serialization.Serializer;

public enum KafkaChannelName {
	CLIENT_BOUND_EVENT("event", ClientBoundEventSerializer.class, ClientBoundEventDeserializer.class),
	TDLIB_REQUEST("request", TdlibRequestSerializer.class, TdlibRequestDeserializer.class),
	TDLIB_RESPONSE("response", TdlibResponseSerializer.class, TdlibResponseDeserializer.class);

	private final String name;
	private final Class<?> serializerClass;
	private final Class<?> deserializerClass;

	KafkaChannelName(String kafkaName,
			Class<?> serializerClass,
			Class<?> deserializerClass) {
		this.name = kafkaName;
		this.serializerClass = serializerClass;
		this.deserializerClass = deserializerClass;
	}

	public String getKafkaName() {
		return name;
	}

	public Class<?> getSerializerClass() {
		return serializerClass;
	}

	public Class<?> getDeserializerClass() {
		return deserializerClass;
	}

	@Override
	public String toString() {
		return name;
	}
}
