package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class KafkaProducer {

	private static final Logger LOG = LogManager.getLogger(KafkaProducer.class);

	private final KafkaSender<Integer, ClientBoundEvent> sender;


	public KafkaProducer(KafkaParameters kafkaParameters) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParameters.bootstrapServers());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaParameters.clientId());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClientBoundEventSerializer.class);
		SenderOptions<Integer, ClientBoundEvent> senderOptions = SenderOptions.create(props);

		sender = KafkaSender.create(senderOptions.maxInFlight(1024));
	}

	public Mono<Void> sendMessages(long userId, Flux<ClientBoundEvent> eventsFlux) {
		return eventsFlux
				.<SenderRecord<Integer, ClientBoundEvent, Integer>>map(event -> SenderRecord.create(new ProducerRecord<>(
						"tdlib.event.%d".formatted(userId),
						event
				), null))
				.transform(sender::send)
				.doOnError(e -> LOG.error("Send failed", e))
				.then();
	}

	public void close() {
		sender.close();
	}
}
