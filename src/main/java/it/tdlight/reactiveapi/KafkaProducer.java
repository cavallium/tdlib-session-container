package it.tdlight.reactiveapi;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public abstract class KafkaProducer<K> {

	private static final Logger LOG = LogManager.getLogger(KafkaProducer.class);

	private final KafkaSender<Integer, K> sender;

	public KafkaProducer(KafkaParameters kafkaParameters) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParameters.bootstrapServers());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaParameters.clientId());
		//props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.ACKS_CONFIG, "0");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getChannelName().getSerializerClass());
		SenderOptions<Integer, K> senderOptions = SenderOptions.create(props);

		sender = KafkaSender.create(senderOptions.maxInFlight(1024));
	}

	public abstract KafkaChannelName getChannelName();

	public Mono<Void> sendMessages(long userId, Flux<K> eventsFlux) {
		var userTopic = new UserTopic(getChannelName(), userId);
		return eventsFlux
				.<SenderRecord<Integer, K, Integer>>map(event -> SenderRecord.create(new ProducerRecord<>(
						userTopic.getTopic(),
						event
				), null))
				.log("produce-messages-" + userTopic,
						Level.FINEST,
						SignalType.REQUEST,
						SignalType.ON_NEXT,
						SignalType.ON_ERROR,
						SignalType.ON_COMPLETE
				)
				.transform(sender::send)
				.doOnError(e -> LOG.error("Send failed", e))
				.then();
	}

	public void close() {
		sender.close();
	}
}
