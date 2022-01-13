package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

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
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ReceiverOptions<Integer, ClientBoundEvent> receiverOptions = ReceiverOptions
				.<Integer, ClientBoundEvent>create(props)
				.commitInterval(Duration.ofSeconds(10));
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

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String groupId, boolean ack, long userId, long liveId) {
		if (ack) {
			return createReceiver(groupId, liveId, userId)
					.receiveAutoAck()
					.flatMapSequential(a -> a)
					.map(ConsumerRecord::value);
		} else {
			return createReceiver(groupId, liveId, userId).receive().map(ConsumerRecord::value);
		}
	}

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String groupId, boolean ack, long userId) {
		if (ack) {
			return createReceiver(groupId, null, userId)
					.receiveAutoAck()
					.flatMapSequential(a -> a)
					.map(ConsumerRecord::value);
		} else {
			return createReceiver(groupId, null, userId).receive().map(ConsumerRecord::value);
		}
	}

	public Flux<ClientBoundEvent> consumeMessages(@NotNull String groupId, boolean ack) {
		if (ack) {
			return createReceiver(groupId, null, null).receiveAutoAck().flatMapSequential(a -> a).map(ConsumerRecord::value);
		} else {
			return createReceiver(groupId, null, null).receive().map(ConsumerRecord::value);
		}
	}

	public String newRandomGroupId() {
		return UUID.randomUUID().toString();
	}
}
