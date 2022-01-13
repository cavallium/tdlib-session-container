package it.tdlight.reactiveapi;

import java.util.stream.Collectors;

public record KafkaParameters(String groupId, String clientId, String bootstrapServers) {

	public KafkaParameters(ClusterSettings clusterSettings, String clientId) {
		this(clientId, clientId, String.join(",", clusterSettings.kafkaBootstrapServers));
	}
}
