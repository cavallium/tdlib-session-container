package it.tdlight.reactiveapi;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record KafkaParameters(String groupId, String clientId, List<String> bootstrapServers,
															List<String> lanes) implements ChannelsParameters {

	public String getBootstrapServersString() {
		return String.join(",", bootstrapServers);
	}

	@Override
	public Set<String> getAllLanes() {
		var lanes = new LinkedHashSet<String>(this.lanes.size() + 1);
		lanes.add("main");
		lanes.addAll(this.lanes);
		return lanes;
	}
}
