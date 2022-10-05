package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Define the cluster structure
 */
public class ClusterSettings {

	public String id;
	public List<String> kafkaBootstrapServers;
	public String rsocketHost;
	public List<String> lanes;

	@JsonCreator
	public ClusterSettings(@JsonProperty(required = true, value = "id") String id,
			@JsonProperty(value = "kafkaBootstrapServers") List<String> kafkaBootstrapServers,
			@JsonProperty(value = "rsocketHost") String rsocketHost,
			@JsonProperty(required = true, value = "lanes") List<String> lanes) {
		this.id = id;
		this.kafkaBootstrapServers = kafkaBootstrapServers;
		this.rsocketHost = rsocketHost;
		this.lanes = lanes;
		if ((rsocketHost == null) == (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty())) {
			throw new IllegalArgumentException("Please configure either RSocket or Kafka");
		}
	}

	public ChannelsParameters toParameters(String clientId, InstanceType instanceType) {
		if (rsocketHost != null) {
			return new RSocketParameters(instanceType, rsocketHost, lanes);
		} else {
			return new KafkaParameters(clientId, clientId, kafkaBootstrapServers, List.copyOf(lanes));
		}
	}
}
