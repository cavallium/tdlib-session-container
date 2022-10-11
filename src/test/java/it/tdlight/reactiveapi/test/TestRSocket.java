package it.tdlight.reactiveapi.test;

import com.google.common.collect.Collections2;
import com.google.common.net.HostAndPort;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.tdlight.reactiveapi.rsocket.MyRSocketClient;
import it.tdlight.reactiveapi.rsocket.MyRSocketServer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class TestRSocket {

	@Test
	public void testClientOnClose() {
		Assertions.assertThrows(IllegalStateException.class, () -> {
			var client = new MyRSocketClient(HostAndPort.fromParts("127.0.0.1", 8085));
			try {
				client.onClose().block(Duration.ofSeconds(1));
			} finally {
				client.dispose();
			}
		});
	}

	@Test
	public void testServerConsumer() {
		var server = new MyRSocketServer(HostAndPort.fromParts("127.0.0.1", 8085), 1);
		try {
			var rawClient = RSocketConnector.create()
					.setupPayload(DefaultPayload.create("client", "setup-info"))
					.payloadDecoder(PayloadDecoder.ZERO_COPY)
					.connect(TcpClientTransport.create("127.0.0.1", 8085))
					.block(Duration.ofSeconds(5));
			Assertions.assertNotNull(rawClient);
			var outputSequence = Flux.just("a", "b", "c").map(DefaultPayload::create);
			var disposable = rawClient
					.requestChannel(Flux.just(DefaultPayload.create("test", "channel")).concatWith(outputSequence))
					.subscribeOn(Schedulers.parallel())
					.subscribe(v -> {}, ex -> Assertions.fail(ex));
			try  {
				EventConsumer<String> consumer = server.registerConsumer(ChannelCodec.UTF8_TEST, "test");
				var events = consumer.consumeMessages().collectList().block(Duration.ofSeconds(5));
				Assertions.assertNotNull(events);
				var mappedEvents = List.copyOf(Collections2.transform(events, Timestamped::data));
				Assertions.assertEquals(List.of("a", "b", "c"), mappedEvents);
			} finally {
				disposable.dispose();
			}
		} finally {
			server.dispose();
		}
	}

	@Test
	public void testServerProducer() {
		var server = new MyRSocketServer(HostAndPort.fromParts("127.0.0.1", 8085), 1);
		try {
			var rawClient = RSocketConnector.create()
					.setupPayload(DefaultPayload.create("client", "setup-info"))
					.payloadDecoder(PayloadDecoder.ZERO_COPY)
					.connect(TcpClientTransport.create("127.0.0.1", 8085))
					.block(Duration.ofSeconds(5));
			Assertions.assertNotNull(rawClient);
			EventProducer<String> producer = server.registerProducer(ChannelCodec.UTF8_TEST, "test");
			var disposable = producer
					.sendMessages(Flux.just("a", "b", "c"))
					.subscribeOn(Schedulers.parallel())
					.subscribe(v -> {}, ex -> Assertions.fail(ex));
			try  {
				var events = rawClient
						.requestStream(DefaultPayload.create("test", "channel"))
						.map(Payload::getDataUtf8)
						.collectList()
						.block();
				Assertions.assertNotNull(events);
				Assertions.assertEquals(List.of("a", "b", "c"), events);
			} finally {
				disposable.dispose();
			}
		} finally {
			server.dispose();
		}
	}

	@Test
	public void testClientConsumer() {
		var client = new MyRSocketClient(HostAndPort.fromParts("127.0.0.1", 8085));
		try {
			var rawServer = RSocketServer.create(SocketAcceptor.forRequestStream(payload -> {
						var metadata = payload.getMetadataUtf8();
						Assertions.assertEquals("channel", metadata);
						var data = payload.getDataUtf8();
						Assertions.assertEquals("test", data);
						return Flux.just("a", "b", "c").map(DefaultPayload::create);
					}))
					.payloadDecoder(PayloadDecoder.ZERO_COPY)
					.bindNow(TcpServerTransport.create("127.0.0.1", 8085));
			try {
				var events = client
						.<String>registerConsumer(ChannelCodec.UTF8_TEST, "test")
						.consumeMessages()
						.map(Timestamped::data)
						.collectList()
						.block(Duration.ofSeconds(5));
				Assertions.assertNotNull(events);
				Assertions.assertEquals(List.of("a", "b", "c"), events);
			} finally {
				rawServer.dispose();
			}
		} finally {
			client.dispose();
		}
	}

	@Test
	public void testClientProducer() {
		AtomicBoolean received = new AtomicBoolean();
		var rawServer = RSocketServer
				.create(SocketAcceptor.forRequestChannel(payloads -> Flux.from(payloads).switchOnFirst((first, flux) -> {
					if (first.isOnNext()) {
						var payload = first.get();
						Assertions.assertNotNull(payload);
						var metadata = payload.getMetadataUtf8();
						Assertions.assertEquals("channel", metadata);
						var data = payload.getDataUtf8();
						Assertions.assertEquals("test", data);
						return flux.skip(1).map(Payload::getDataUtf8).collectList().doOnSuccess(val -> {
							received.set(true);
							Assertions.assertEquals(List.of("a", "b", "c"), val);
						}).then(Mono.just(DefaultPayload.create("ok", "response")));
					} else {
						return flux.take(1, true);
					}
				})))
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				.bindNow(TcpServerTransport.create("127.0.0.1", 8085));
		try {
			var client = new MyRSocketClient(HostAndPort.fromParts("127.0.0.1", 8085));
			try {
				client
						.<String>registerProducer(ChannelCodec.UTF8_TEST, "test")
						.sendMessages(Flux.just("a", "b", "c"))
						.block(Duration.ofMinutes(1));
				Assertions.assertTrue(received.get());
			} finally {
				client.dispose();
			}
		} finally {
			rawServer.dispose();
		}
	}

	@Test
	public void testServerOnClose() {
		Assertions.assertThrows(IllegalStateException.class, () -> {
			var server = new MyRSocketServer(HostAndPort.fromParts("127.0.0.1", 8085), 1);
			try {
				server.onClose().block(Duration.ofSeconds(1));
			} finally {
				server.dispose();
			}
		});
	}
}
