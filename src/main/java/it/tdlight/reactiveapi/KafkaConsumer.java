package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

public class KafkaConsumer {

	private static final Logger LOG = LogManager.getLogger(KafkaConsumer.class);

	private final KafkaParameters kafkaParameters;

	public KafkaConsumer(KafkaParameters kafkaParameters) {
		this.kafkaParameters = kafkaParameters;
	}

	public KafkaReceiver<Integer, ClientBoundEvent> createReceiver(@NotNull String groupId,
			@Nullable Long liveId,
			@Nullable Long userId) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParameters.bootstrapServers());
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaParameters.clientId());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ClientBoundEventDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		ReceiverOptions<Integer, ClientBoundEvent> receiverOptions = ReceiverOptions
				.<Integer, ClientBoundEvent>create(props)
				.commitInterval(Duration.ofSeconds(10))
				.commitBatchSize(64)
				.maxCommitAttempts(100)
				.maxDeferredCommits(100);
		Pattern pattern;
		if (liveId == null && userId == null) {
			pattern = Pattern.compile("tdlib\\.event\\.[0-9]+\\.[0-9]+");
		} else if (liveId == null) {
			pattern = Pattern.compile("tdlib\\.event\\." + userId + "\\.[0-9]+");
		} else if (userId == null) {
			pattern = Pattern.compile("tdlib\\.event\\.[0-9]+\\." + liveId);
		} else {
			pattern = Pattern.compile("tdlib\\.event\\." + userId + "\\." + liveId);
		}
		ReceiverOptions<Integer, ClientBoundEvent> options = receiverOptions
				.subscription(pattern)
				.addAssignListener(partitions -> LOG.debug("onPartitionsAssigned {}", partitions))
				.addRevokeListener(partitions -> LOG.debug("onPartitionsRevoked {}", partitions));
		return KafkaReceiver.create(options);
	}

	private Flux<ClientBoundEvent> retryIfCleanup(Flux<ClientBoundEvent> clientBoundEventFlux) {
		return clientBoundEventFlux.retryWhen(Retry
				.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
				.maxBackoff(Duration.ofSeconds(5))
				.filter(ex -> ex instanceof RebalanceInProgressException)
				.doBeforeRetry(s -> LOG.warn("Rebalancing in progress")));
	}

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String subGroupId, boolean ack, long userId, long liveId) {
		if (ack) {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, liveId, userId)
					.receive()
					.log("consume-messages", Level.FINEST, SignalType.REQUEST)
					.doOnNext(result -> result.receiverOffset().acknowledge())
					.map(ConsumerRecord::value)
					.transform(this::retryIfCleanup);
		} else {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, liveId, userId).receive().map(ConsumerRecord::value);
		}
	}

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String subGroupId, boolean ack, long userId) {
		if (ack) {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, null, userId)
					.receive()
					.log("consume-messages", Level.FINEST, SignalType.REQUEST)
					.doOnNext(result -> result.receiverOffset().acknowledge())
					.map(ConsumerRecord::value)
					.transform(this::retryIfCleanup);
		} else {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, null, userId).receive().map(ConsumerRecord::value);
		}
	}

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String subGroupId, boolean ack) {
		if (ack) {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, null, null)
					.receive()
					.log("consume-messages", Level.FINEST, SignalType.REQUEST)
					.doOnNext(result -> result.receiverOffset().acknowledge())
					.map(ConsumerRecord::value)
					.transform(this::retryIfCleanup);
		} else {
			return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, null, null).receive().map(ConsumerRecord::value);
		}
	}
}
