package it.tdlight.reactiveapi.test;

import com.google.common.net.HostAndPort;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestServerToClientChannel extends TestChannel {

	@Override
	public boolean isConsumerClient() {
		return true;
	}
}
