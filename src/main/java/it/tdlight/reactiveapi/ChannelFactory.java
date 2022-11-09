package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.kafka.KafkaConsumer;
import it.tdlight.reactiveapi.kafka.KafkaProducer;
import it.tdlight.reactiveapi.rsocket.MyRSocketClient;
import it.tdlight.reactiveapi.rsocket.MyRSocketServer;
import it.tdlight.reactiveapi.rsocket.RSocketChannelManager;
import java.io.Closeable;
import java.io.IOException;

public interface ChannelFactory {

	static ChannelFactory getFactoryFromParameters(ChannelsParameters channelsParameters) {
		if (channelsParameters instanceof KafkaParameters kafkaParameters) {
			return new KafkaChannelFactory(kafkaParameters);
		} else if (channelsParameters instanceof RSocketParameters socketParameters) {
			return new RSocketChannelFactory(socketParameters);
		} else {
			throw new UnsupportedOperationException("Unsupported parameters type: " + channelsParameters);
		}
	}

	<T> EventConsumer<T> newConsumer(boolean quickResponse, ChannelCodec channelCodec, String channelName);

	<T> EventProducer<T> newProducer(ChannelCodec channelCodec, String channelName);

	class KafkaChannelFactory implements ChannelFactory {

		private final KafkaParameters channelsParameters;

		public KafkaChannelFactory(KafkaParameters channelsParameters) {
			this.channelsParameters = channelsParameters;
		}

		@Override
		public <T> EventConsumer<T> newConsumer(boolean quickResponse, ChannelCodec channelCodec, String channelName) {
			return new KafkaConsumer<>(channelsParameters, quickResponse, channelCodec, channelName);
		}

		@Override
		public <T> EventProducer<T> newProducer(ChannelCodec channelCodec, String channelName) {
			return new KafkaProducer<>(channelsParameters, channelCodec, channelName);
		}
	}

	class RSocketChannelFactory implements ChannelFactory, Closeable {

		private final RSocketParameters channelsParameters;
		private final RSocketChannelManager manager;

		public RSocketChannelFactory(RSocketParameters channelsParameters) {
			this.channelsParameters = channelsParameters;
			if (channelsParameters.isClient()) {
				this.manager = new MyRSocketClient(channelsParameters.transportFactory());
			} else {
				this.manager = new MyRSocketServer(channelsParameters.transportFactory());
			}
		}

		@Override
		public <T> EventConsumer<T> newConsumer(boolean quickResponse, ChannelCodec channelCodec, String channelName) {
			return manager.registerConsumer(channelCodec, channelName);
		}

		@Override
		public <T> EventProducer<T> newProducer(ChannelCodec channelCodec, String channelName) {
			return manager.registerProducer(channelCodec, channelName);
		}

		@Override
		public void close() throws IOException {
			manager.dispose();
			manager.onClose().block();
		}
	}
}
