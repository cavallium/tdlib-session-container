package it.tdlight.reactiveapi.test;

import com.google.common.net.HostAndPort;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.Timestamped;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class TestChannel {

	protected HostAndPort hostAndPort;
	protected EventConsumer<String> consumer;
	protected EventProducer<String> producer;
	protected IntArrayList data;

	@BeforeEach
	public void beforeEach() {
		hostAndPort = HostAndPort.fromParts("localhost", 25689);
		consumer = createConsumer(hostAndPort, false);
		producer = createProducer(hostAndPort, false);
		data = new IntArrayList(100);
		for (int i = 0; i < 100; i++) {
			data.add(i);
		}
	}

	public abstract EventConsumer<String> createConsumer(HostAndPort hostAndPort, boolean bomb);

	public abstract EventProducer<String> createProducer(HostAndPort hostAndPort, boolean bomb);

	@AfterEach
	public void afterEach() {
		producer.close();
	}

	@Test
	public void testSimple() {
		Mono<IntArrayList> sender = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString))
				.then(Mono.empty());
		Mono<IntArrayList> receiver = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isServerSender() ? (List.of(sender, receiver)) : List.of(receiver, sender))
				.blockLast();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testException() {
		Mono<IntArrayList> sender = producer
				.sendMessages(Flux.concat(
						Flux.fromIterable(data).map(Integer::toUnsignedString),
						Mono.error(new FakeException())
				))
				.then(Mono.empty());
		Mono<IntArrayList> receiver = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		Assertions.assertThrows(Exception.class, () -> Flux
				.merge(isServerSender() ? (List.of(sender, receiver)) : List.of(receiver, sender))
				.blockLast());
	}

	@Test
	public void testEmpty() {
		Mono<IntArrayList> sender = producer
				.sendMessages(Flux.empty())
				.then(Mono.empty());
		Mono<IntArrayList> receiver = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var data = Flux
				.merge(isServerSender() ? (List.of(sender, receiver)) : List.of(receiver, sender))
				.blockLast();
		Assertions.assertNotNull(data);
		Assertions.assertTrue(data.isEmpty());
	}

	@Test
	public void testSimpleOneByOne() {
		var sender = producer.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var receiver = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isServerSender() ? (List.of(sender, receiver)) : List.of(receiver, sender))
				.blockLast();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testCancel() {
		var sender = producer.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var receiver = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.take(50, true)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isServerSender() ? (List.of(sender, receiver)) : List.of(receiver, sender))
				.blockLast();
		data.removeElements(50, 100);
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testConsumeDelay() {
		var sender = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var receiver = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.concatMap(item -> item == 15 ? Mono.just(item).delaySubscription(Duration.ofSeconds(8)) : Mono.just(item))
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isServerSender() ? List.of(sender, receiver) : List.of(receiver, sender))
				.blockLast();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testProduceDelay() {
		var sender = producer
				.sendMessages(Flux.fromIterable(data)
						.concatMap(item -> item == 15 ? Mono.just(item).delaySubscription(Duration.ofSeconds(8)) : Mono.just(item))
						.map(Integer::toUnsignedString));
		var receiver = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isServerSender() ? List.of(sender, receiver) : List.of(receiver, sender))
				.blockLast();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testConsumeMidCancel() {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		Mono<IntArrayList> sender = producer
				.sendMessages(dataFlux.map(Integer::toUnsignedString))
				.then(Mono.empty());
		var receiver1 = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.take(10, true)
				.collect(Collectors.toCollection(IntArrayList::new));
		var receiverWait =  Flux.<IntArrayList>empty().delaySubscription(Duration.ofSeconds(4));
		var receiver2 = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		Flux<IntArrayList> part1 = Flux
				.merge((isServerSender() ? sender : receiver1), isServerSender() ? receiver1 : sender);
		Flux<IntArrayList> part2 = Flux
				.merge((isServerSender() ? sender : receiver2), receiverWait.then(isServerSender() ? receiver2 : sender));
		var response = Flux.concat(part1, part2).reduce((a, b) -> {
			a.addAll(b);
			return a;
		}).block();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	public abstract boolean isServerSender();
}
