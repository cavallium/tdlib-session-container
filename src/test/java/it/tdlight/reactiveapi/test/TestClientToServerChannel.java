package it.tdlight.reactiveapi.test;

import com.google.common.net.HostAndPort;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;

public class TestClientToServerChannel extends TestChannel {

	@Override
	public boolean isConsumerClient() {
		return false;
	}

}
