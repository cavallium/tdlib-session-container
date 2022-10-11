package it.tdlight.reactiveapi.test;

import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.ChannelFactory;
import it.tdlight.reactiveapi.ChannelFactory.RSocketChannelFactory;
import it.tdlight.reactiveapi.EventConsumer;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.RSocketParameters;
import it.tdlight.reactiveapi.Timestamped;
import it.tdlight.reactiveapi.rsocket.CancelledChannelException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

public abstract class TestChannel {

	private static final Logger LOG = LogManager.getLogger(TestChannel.class);

	protected ChannelFactory channelFactory;
	protected IntArrayList data;
	protected ConcurrentLinkedDeque<Closeable> closeables = new ConcurrentLinkedDeque<>();
	protected Function<String, EventConsumer<String>> consumerFactory;
	protected Function<String, EventProducer<String>> producerFactory;
	private EventConsumer<String> consumer;
	private EventProducer<String> producer;

	@BeforeEach
	public void beforeEach() {
		var consumerFactory = new RSocketChannelFactory(new RSocketParameters(isConsumerClient(), "127.0.0.1:25689", List.of()));
		var producerFactory = new RSocketChannelFactory(new RSocketParameters(!isConsumerClient(), "127.0.0.1:25689", List.of()));

		closeables.offer(consumerFactory);
		closeables.offer(producerFactory);
		this.consumerFactory = name -> consumerFactory.newConsumer(true, ChannelCodec.UTF8_TEST, name);
		this.producerFactory = name -> {
			EventProducer<String> p = producerFactory.newProducer(ChannelCodec.UTF8_TEST, name);
			closeables.addFirst(p::close);
			return p;
		};
		consumer = this.consumerFactory.apply("test");
		producer = this.producerFactory.apply("test");
		data = new IntArrayList(100);
		for (int i = 0; i < 100; i++) {
			data.add(i);
		}
	}

	@AfterEach
	public void afterEach() {
		System.out.println("Cleaning up...");
		producer.close();
		while (!closeables.isEmpty()) {
			var c = closeables.poll();
			try {
				c.close();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testProducerEndsSuccessfully() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString))
				.then(Mono.<IntArrayList>empty())
				.timeout(Duration.ofSeconds(5));
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast();
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testConsumerEndsSuccessfully() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString))
				.then(Mono.empty());
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new))
				.timeout(Duration.ofSeconds(5));
		var response = Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(5));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testSimple() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString))
				.doOnSuccess(s -> LOG.warn("Done producer"))
				.then(Mono.<IntArrayList>empty());
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new))
				.doOnSuccess(s -> LOG.warn("Done consumer"));
		var response = Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(5));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testInvertedSubscription() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString))
				.doOnSuccess(s -> LOG.warn("Done producer"))
				.then(Mono.empty());
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new))
				.doOnSuccess(s -> LOG.warn("Done consumer"));
		var response = Flux
				.merge(isConsumerClient() ? List.of(eventConsumer, eventProducer.delaySubscription(Duration.ofSeconds(1)))
						: List.of(eventProducer, eventConsumer.delaySubscription(Duration.ofSeconds(1))))
				.blockLast(Duration.ofSeconds(60));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testException() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.concat(
						Flux.fromIterable(data).map(Integer::toUnsignedString),
						Mono.error(new FakeException())
				))
				.then(Mono.empty());
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		Assertions.assertThrows(Exception.class, () -> Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(5)));
	}

	@Test
	public void testEmpty() {
		Mono<IntArrayList> eventProducer = producer
				.sendMessages(Flux.empty())
				.then(Mono.empty());
		Mono<IntArrayList> eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var data = Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(5));
		Assertions.assertNotNull(data);
		Assertions.assertTrue(data.isEmpty());
	}

	@Test
	public void testSimpleOneByOne() {
		var eventProducer = producer.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var eventConsumer = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(5));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testCancel() {
		var eventProducer = producer.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var eventConsumer = consumer
				.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.take(50, true)
				.collect(Collectors.toCollection(IntArrayList::new));
		Assertions.assertThrows(Throwable.class, () -> {
			var response = Flux
					.merge(isConsumerClient() ? (List.of(eventProducer, eventConsumer)) : List.of(eventConsumer, eventProducer))
					.blockLast(Duration.ofSeconds(5));
			data.removeElements(50, 100);
			Assertions.assertEquals(response, data);
			System.out.println(response);
		});
	}

	@Test
	public void testConsumeDelay() {
		var eventProducer = producer
				.sendMessages(Flux.fromIterable(data).map(Integer::toUnsignedString));
		var eventConsumer = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.concatMap(item -> item == 15 ? Mono.just(item).delaySubscription(Duration.ofSeconds(8)) : Mono.just(item))
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isConsumerClient() ? List.of(eventProducer, eventConsumer) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(12));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testProduceDelay() {
		var eventProducer = producer
				.sendMessages(Flux.fromIterable(data)
						.concatMap(item -> item == 15 ? Mono.just(item).delaySubscription(Duration.ofSeconds(8)) : Mono.just(item))
						.map(Integer::toUnsignedString));
		var eventConsumer = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.collect(Collectors.toCollection(IntArrayList::new));
		var response = Flux
				.merge(isConsumerClient() ? List.of(eventProducer, eventConsumer) : List.of(eventConsumer, eventProducer))
				.blockLast(Duration.ofSeconds(12));
		Assertions.assertEquals(response, data);
		System.out.println(response);
	}

	@Test
	public void testConsumeMidCancel() {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		AtomicReference<Throwable> exRef = new AtomicReference<>();
		var eventProducer = producer
				.sendMessages(dataFlux.map(Integer::toUnsignedString))
				.retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, exRef::set);
		try {
			var receiver1 = consumer
					.consumeMessages()
					.limitRate(1)
					.map(Timestamped::data)
					.map(Integer::parseUnsignedInt)
					.take(10, true)
					.collect(Collectors.toCollection(IntArrayList::new))
					.block(Duration.ofSeconds(5));
			var receiver2 = consumer
					.consumeMessages()
					.limitRate(1)
					.map(Timestamped::data)
					.map(Integer::parseUnsignedInt)
					.collect(Collectors.toCollection(IntArrayList::new))
					.block(Duration.ofSeconds(5));
			var ex = exRef.get();
			if (ex != null) {
				Assertions.fail(ex);
			}
			Assertions.assertNotNull(receiver1);
			Assertions.assertNotNull(receiver2);
			Assertions.assertEquals(data.subList(0, 10), receiver1);
			Assertions.assertEquals(data.subList(50, 100), receiver2.subList(receiver2.size() - 50, receiver2.size()));
			System.out.println(receiver1);
		} finally {
			eventProducer.dispose();
		}
	}

	@Test
	public void testConsumeMidFail() {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		var eventProducer = producer
				.sendMessages(dataFlux.map(Integer::toUnsignedString))
				.retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> Assertions.fail(ex));
		try {
			Assertions.assertThrows(FakeException.class, () -> {
				consumer
						.consumeMessages()
						.limitRate(1)
						.map(Timestamped::data)
						.map(Integer::parseUnsignedInt)
						.doOnNext(n -> {
							if (n == 10) {
								throw new FakeException();
							}
						})
						.collect(Collectors.toCollection(IntArrayList::new))
						.block();
			});
			var receiver2 = consumer
					.consumeMessages()
					.limitRate(1)
					.map(Timestamped::data)
					.map(Integer::parseUnsignedInt)
					.collect(Collectors.toCollection(IntArrayList::new))
					.block();
			Assertions.assertNotNull(receiver2);
			Assertions.assertNotEquals(0, receiver2.getInt(0));
			Assertions.assertNotEquals(1, receiver2.getInt(1));
			Assertions.assertNotEquals(2, receiver2.getInt(2));
			System.out.println(receiver2);
		} finally {
			eventProducer.dispose();
		}
	}

	@Test
	public void testProduceMidCancel() {
		var dataFlux = Flux.fromIterable(data).publish(1).autoConnect();
		var numbers = new ConcurrentLinkedDeque<Integer>();
		var eventConsumer = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.doOnNext(numbers::offer)
				.retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> Assertions.fail(ex));
		try {
			producer
					.sendMessages(dataFlux.limitRate(1).take(10, true).map(Integer::toUnsignedString))
					.block();
			producer
					.sendMessages(dataFlux.limitRate(1).map(Integer::toUnsignedString))
					.block();
			if (numbers.size() < data.size()) {
				data.removeInt(data.size() - 1);
			}
			Assertions.assertEquals(data, List.copyOf(numbers));
		} finally {
			eventConsumer.dispose();
		}
	}

	@Test
	public void testProduceMidFail() {
		var dataFlux = Flux.fromIterable(data).publish(1).autoConnect();
		var numbers = new ConcurrentLinkedDeque<Integer>();
		var eventConsumer = consumer
				.consumeMessages()
				.limitRate(1)
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.doOnNext(numbers::offer)
				.retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.parallel())
				.subscribe(n -> {}, ex -> Assertions.fail(ex));
		try {
			Assertions.assertThrows(FakeException.class, () -> {
				producer
						.sendMessages(dataFlux.limitRate(1).doOnNext(i -> {
							if (i == 10) {
								throw new FakeException();
							}
						}).map(Integer::toUnsignedString))
						.block(Duration.ofSeconds(5));
			});
			producer
					.sendMessages(dataFlux.limitRate(1).map(Integer::toUnsignedString))
					.block(Duration.ofSeconds(5));
			Assertions.assertTrue(numbers.contains(0));
			Assertions.assertTrue(numbers.contains(1));
			Assertions.assertTrue(numbers.contains(50));
			Assertions.assertTrue(numbers.contains(51));
		} finally {
			eventConsumer.dispose();
		}
	}

	@Test
	public void testResubscribe() throws InterruptedException {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		var eventProducer = producer
				.sendMessages(dataFlux.map(Integer::toUnsignedString))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.retryWhen(Retry.fixedDelay(5, Duration.ofSeconds(1)))
				.subscribe(n -> {}, ex -> Assertions.fail(ex));
		try {
			consumer
					.consumeMessages()
					.limitRate(1)
					.map(Timestamped::data)
					.map(Integer::parseUnsignedInt)
					.take(10, true)
					.log("consumer-1", Level.INFO)
					.blockLast(Duration.ofSeconds(5));

			Thread.sleep(4000);

			consumer
					.consumeMessages()
					.limitRate(1)
					.map(Timestamped::data)
					.map(Integer::parseUnsignedInt)
					.log("consumer-2", Level.INFO)
					.blockLast(Duration.ofSeconds(5));
		} finally {
			eventProducer.dispose();
		}
	}

	@Test
	public void testFailTwoSubscribers() {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		var eventProducer = producer
				.sendMessages(dataFlux.map(Integer::toUnsignedString))
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.retryWhen(Retry.fixedDelay(5, Duration.ofSeconds(1)))
				.subscribe(n -> {}, ex -> Assertions.fail(ex));

		Assertions.assertThrows(IllegalStateException.class, () -> {
			try {
				Mono
						.when(consumer
										.consumeMessages()
										.limitRate(1)
										.map(Timestamped::data)
										.map(Integer::parseUnsignedInt)
										.log("consumer-1", Level.INFO)
										.doOnError(ex -> Assertions.fail(ex)),
								consumer
										.consumeMessages()
										.limitRate(1)
										.map(Timestamped::data)
										.map(Integer::parseUnsignedInt)
										.log("consumer-2", Level.INFO)
										.onErrorResume(io.rsocket.exceptions.ApplicationErrorException.class,
												ex -> Mono.error(new IllegalStateException(ex))
										)
						)
						.block();
			} catch (RuntimeException ex) {
				throw Exceptions.unwrap(ex);
			} finally {
				eventProducer.dispose();
			}
		});
	}

	@Test
	public void testRepublish() throws InterruptedException {
		var dataFlux = Flux.fromIterable(data).publish().autoConnect();
		var eventConsumer = consumer.consumeMessages()
				.map(Timestamped::data)
				.map(Integer::parseUnsignedInt)
				.repeatWhen(n -> n.delayElements(Duration.ofSeconds(1)))
				.retryWhen(Retry.fixedDelay(5, Duration.ofSeconds(1)))
				.subscribe(n -> {}, ex -> Assertions.fail(ex));
		try {
			producer
					.sendMessages(dataFlux
							.take(10, true)
							.log("producer-1", Level.INFO)
							.map(Integer::toUnsignedString))
					.block(Duration.ofSeconds(5));
			Thread.sleep(4000);
			producer
					.sendMessages(dataFlux.log("producer-2", Level.INFO).map(Integer::toUnsignedString))
					.block(Duration.ofSeconds(5));
		} finally {
			eventConsumer.dispose();
		}
	}

	public abstract boolean isConsumerClient();
}
