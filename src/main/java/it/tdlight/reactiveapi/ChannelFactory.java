package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.kafka.KafkaConsumer;
import it.tdlight.reactiveapi.kafka.KafkaProducer;

public interface ChannelFactory {

	<T> EventConsumer<T> newConsumer(KafkaParameters kafkaParameters,
			boolean quickResponse,
			ChannelCodec channelCodec,
			String channelName);

	<T> EventProducer<T> newProducer(KafkaParameters kafkaParameters,
			ChannelCodec channelCodec,
			String channelName);

	class KafkaChannelFactory implements ChannelFactory {

		@Override
		public <T> EventConsumer<T> newConsumer(KafkaParameters kafkaParameters,
				boolean quickResponse,
				ChannelCodec channelCodec,
				String channelName) {
			return new KafkaConsumer<>(kafkaParameters, quickResponse, channelCodec, channelName);
		}

		@Override
		public <T> EventProducer<T> newProducer(KafkaParameters kafkaParameters,
				ChannelCodec channelCodec,
				String channelName) {
			return new KafkaProducer<>(kafkaParameters, channelCodec, channelName);
		}
	}
}
