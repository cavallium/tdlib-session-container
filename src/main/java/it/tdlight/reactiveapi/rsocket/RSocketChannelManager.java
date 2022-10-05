package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Closeable;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;

public interface RSocketChannelManager extends Closeable {

	<K> EventConsumer<K> registerConsumer(ChannelCodec channelCodec, String channelName);

	<K> EventProducer<K> registerProducer(ChannelCodec channelCodec, String channelName);
}
