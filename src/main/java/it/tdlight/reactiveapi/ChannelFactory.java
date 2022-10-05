package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.kafka.KafkaConsumer;
import it.tdlight.reactiveapi.kafka.KafkaProducer;
import it.tdlight.reactiveapi.rsocket.RSocketConsumeAsClient;
import it.tdlight.reactiveapi.rsocket.RSocketProduceAsServer;
import it.tdlight.reactiveapi.rsocket.RSocketConsumeAsServer;
import it.tdlight.reactiveapi.rsocket.RSocketProduceAsClient;

public interface ChannelFactory {

	static ChannelFactory getFactoryFromParameters(ChannelsParameters channelsParameters) {
		if (channelsParameters instanceof KafkaParameters) {
			return new KafkaChannelFactory();
		} else {
			return new RsocketChannelFactory();
		}
	}

	<T> EventConsumer<T> newConsumer(ChannelsParameters channelsParameters,
			boolean quickResponse,
			ChannelCodec channelCodec,
			String channelName);

	<T> EventProducer<T> newProducer(ChannelsParameters channelsParameters,
			ChannelCodec channelCodec,
			String channelName);

	class KafkaChannelFactory implements ChannelFactory {

		@Override
		public <T> EventConsumer<T> newConsumer(ChannelsParameters channelsParameters,
				boolean quickResponse,
				ChannelCodec channelCodec,
				String channelName) {
			return new KafkaConsumer<>((KafkaParameters) channelsParameters, quickResponse, channelCodec, channelName);
		}

		@Override
		public <T> EventProducer<T> newProducer(ChannelsParameters channelsParameters,
				ChannelCodec channelCodec,
				String channelName) {
			return new KafkaProducer<>((KafkaParameters) channelsParameters, channelCodec, channelName);
		}
	}

	class RsocketChannelFactory implements ChannelFactory {

		@Override
		public <T> EventConsumer<T> newConsumer(ChannelsParameters channelsParameters,
				boolean quickResponse,
				ChannelCodec channelCodec,
				String channelName) {
			var socketParameters = (RSocketParameters) channelsParameters;
			if (socketParameters.isClient()) {
				return new RSocketConsumeAsClient<>(socketParameters.host(), channelCodec, channelName);
			} else {
				return new RSocketConsumeAsServer<>(socketParameters.host(), channelCodec, channelName);
			}
		}

		@Override
		public <T> EventProducer<T> newProducer(ChannelsParameters channelsParameters,
				ChannelCodec channelCodec,
				String channelName) {
			var socketParameters = (RSocketParameters) channelsParameters;
			if (socketParameters.isClient()) {
				return new RSocketProduceAsServer<>(socketParameters.host(), channelCodec, channelName);
			} else {
				return new RSocketProduceAsClient<>(socketParameters.host(), channelCodec, channelName);
			}
		}
	}
}
