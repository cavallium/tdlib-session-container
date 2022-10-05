package it.tdlight.reactiveapi.test;

import com.google.common.net.HostAndPort;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.rsocket.RSocketConsumeAsServer;
import it.tdlight.reactiveapi.rsocket.RSocketProduceAsClient;

public class TestClientToServerChannel extends TestChannel {

	@Override
	public EventConsumer<String> createConsumer(HostAndPort hostAndPort, boolean bomb) {
		ChannelCodec codec = ChannelCodec.UTF8_TEST;
		if (bomb) {
			codec = new ChannelCodec(codec.getSerializerClass(), BombDeserializer.class);
		}
		return new RSocketConsumeAsServer<>(hostAndPort, codec, "test");
	}

	@Override
	public EventProducer<String> createProducer(HostAndPort hostAndPort, boolean bomb) {
		ChannelCodec codec = ChannelCodec.UTF8_TEST;
		if (bomb) {
			codec = new ChannelCodec(BombSerializer.class, codec.getDeserializerClass());
		}
		return new RSocketProduceAsClient<>(hostAndPort, codec, "test");
	}

	@Override
	public boolean isServerSender() {
		return false;
	}

}
