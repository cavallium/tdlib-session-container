package it.tdlight.reactiveapi;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record KafkaParameters(String groupId, String clientId, String bootstrapServers, List<String> lanes) {

	public KafkaParameters(ClusterSettings clusterSettings, String clientId) {
		this(clientId,
				clientId,
				String.join(",", clusterSettings.kafkaBootstrapServers),
				List.copyOf(clusterSettings.lanes)
		);
	}

	public Set<String> getAllLanes() {
		var lanes = new LinkedHashSet<String>(this.lanes.size() + 1);
		lanes.add("main");
		lanes.addAll(this.lanes);
		return lanes;
	}
}
