package it.tdlight.reactiveapi.test;

import com.google.common.net.HostAndPort;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.tdlight.reactiveapi.rsocket.RSocketConsumeAsClient;
import it.tdlight.reactiveapi.rsocket.RSocketProduceAsServer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestServerToClientChannel extends TestChannel {

	@Override
	public EventConsumer<String> createConsumer(HostAndPort hostAndPort, boolean bomb) {
		ChannelCodec codec = ChannelCodec.UTF8_TEST;
		if (bomb) {
			codec = new ChannelCodec(codec.getSerializerClass(), BombDeserializer.class);
		}
		return new RSocketConsumeAsClient<>(hostAndPort, codec, "test");
	}

	@Override
	public EventProducer<String> createProducer(HostAndPort hostAndPort, boolean bomb) {
		ChannelCodec codec = ChannelCodec.UTF8_TEST;
		if (bomb) {
			codec = new ChannelCodec(BombSerializer.class, codec.getDeserializerClass());
		}
		return new RSocketProduceAsServer<>(hostAndPort, codec, "test");
	}

	@Override
	public boolean isServerSender() {
		return true;
	}
}
