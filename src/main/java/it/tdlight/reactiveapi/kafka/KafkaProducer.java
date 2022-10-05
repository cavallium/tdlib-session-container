package it.tdlight.reactiveapi.kafka;

import it.tdlight.reactiveapi.ChannelCodec;
import it.tdlight.reactiveapi.EventProducer;
import it.tdlight.reactiveapi.KafkaParameters;
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

public final class KafkaProducer<K> implements EventProducer<K> {

	private static final Logger LOG = LogManager.getLogger(KafkaProducer.class);

	private final KafkaSender<Integer, K> sender;
	private final ChannelCodec channelCodec;
	private final String channelName;

	public KafkaProducer(KafkaParameters kafkaParameters, ChannelCodec channelCodec, String channelName) {
		this.channelCodec = channelCodec;
		this.channelName = channelName;
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParameters.getBootstrapServersString());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaParameters.clientId());
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getChannelCodec().getSerializerClass());
		SenderOptions<Integer, K> senderOptions = SenderOptions.create(props);

		sender = KafkaSender.create(senderOptions.maxInFlight(1024));
	}

	@Override
	public ChannelCodec getChannelCodec() {
		return channelCodec;
	}

	@Override
	public String getChannelName() {
		return channelName;
	}

	@Override
	public Mono<Void> sendMessages(Flux<K> eventsFlux) {
		var channelName = getChannelName();
		return eventsFlux
				.<SenderRecord<Integer, K, Integer>>map(event ->
						SenderRecord.create(new ProducerRecord<>("tdlib." + channelName, event), null))
				.log("produce-messages-" + channelName,
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

	@Override
	public void close() {
		sender.close();
	}
}
