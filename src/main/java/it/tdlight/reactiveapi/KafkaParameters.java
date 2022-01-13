package it.tdlight.reactiveapi;

import java.util.stream.Collectors;

public record KafkaParameters(String clientId, String bootstrapServers) {

	public KafkaParameters(ClusterSettings clusterSettings, String clientId) {
		this(clientId, String.join(",", clusterSettings.kafkaBootstrapServers));
	}
}
