package it.tdlight.reactiveapi;

import static java.lang.Math.toIntExact;

import it.tdlight.common.Init;
import it.tdlight.common.utils.CantLoadLibrary;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

public abstract class KafkaConsumer<K> {

	private static final Logger LOG = LogManager.getLogger(KafkaConsumer.class);

	private final KafkaParameters kafkaParameters;

	public KafkaConsumer(KafkaParameters kafkaParameters) {
		this.kafkaParameters = kafkaParameters;
	}

	public KafkaReceiver<Integer, K> createReceiver(@NotNull String groupId, @Nullable Long userId) {
		try {
			Init.start();
		} catch (CantLoadLibrary e) {
			LOG.error("Can't load TDLight library", e);
			throw new RuntimeException(e);
		}
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParameters.bootstrapServers());
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaParameters.clientId() + (userId != null ? ("_" + userId) : ""));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getChannelName().getDeserializerClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, toIntExact(Duration.ofMinutes(5).toMillis()));
		if (isQuickResponse()) {
			props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
		} else {
			props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
			props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
		}
		ReceiverOptions<Integer, K> receiverOptions = ReceiverOptions.create(props);
		Pattern pattern;
		if (userId == null) {
			pattern = Pattern.compile("tdlib\\." + getChannelName() + "\\.\\d+");
		} else {
			pattern = Pattern.compile("tdlib\\." + getChannelName() + "\\." + userId);
		}
		ReceiverOptions<Integer, K> options = receiverOptions
				.subscription(pattern)
				.addAssignListener(partitions -> LOG.debug("onPartitionsAssigned {}", partitions))
				.addRevokeListener(partitions -> LOG.debug("onPartitionsRevoked {}", partitions));
		return KafkaReceiver.create(options);
	}

	public abstract KafkaChannelName getChannelName();

	public abstract boolean isQuickResponse();

	protected Flux<Timestamped<K>> retryIfCleanup(Flux<Timestamped<K>> eventFlux) {
		return eventFlux.retryWhen(Retry
				.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
				.maxBackoff(Duration.ofSeconds(5))
				.transientErrors(true)
				.filter(ex -> ex instanceof RebalanceInProgressException)
				.doBeforeRetry(s -> LOG.warn("Rebalancing in progress")));
	}

	protected Flux<Timestamped<K>> retryIfCommitFailed(Flux<Timestamped<K>> eventFlux) {
		return eventFlux.retryWhen(Retry
				.backoff(10, Duration.ofSeconds(1))
				.maxBackoff(Duration.ofSeconds(5))
				.transientErrors(true)
				.filter(ex -> ex instanceof CommitFailedException)
				.doBeforeRetry(s -> LOG.warn("Commit cannot be completed since the group has already rebalanced"
						+ " and assigned the partitions to another member. This means that the time between subsequent"
						+ " calls to poll() was longer than the configured max.poll.interval.ms, which typically implies"
						+ " that the poll loop is spending too much time message processing. You can address this either"
						+ " by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll()"
						+ " with max.poll.records.")));
	}

	public Flux<Timestamped<K>> consumeMessages(@NotNull String subGroupId, long userId) {
		return consumeMessagesInternal(subGroupId, userId);
	}

	public Flux<Timestamped<K>> consumeMessages(@NotNull String subGroupId) {
		return consumeMessagesInternal(subGroupId, null);
	}

	private Flux<Timestamped<K>> consumeMessagesInternal(@NotNull String subGroupId, @Nullable Long userId) {
		return createReceiver(kafkaParameters.groupId() + "-" + subGroupId, userId)
				.receive()
				.log("consume-messages" + (userId != null ? "-" + userId : ""),
						Level.FINEST,
						SignalType.REQUEST,
						SignalType.ON_NEXT,
						SignalType.ON_ERROR,
						SignalType.ON_COMPLETE
				)
				//.doOnNext(result -> result.receiverOffset().acknowledge())
				.map(record -> {
					if (record.timestampType() == TimestampType.CREATE_TIME) {
						return new Timestamped<>(record.timestamp(), record.value());
					} else {
						return new Timestamped<>(1, record.value());
					}
				})
				.transform(this::retryIfCleanup)
				.transform(this::retryIfCommitFailed);
	}
}
