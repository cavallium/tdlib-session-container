package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Define the cluster structure
 */
public class ClusterSettings {

	public String id;
	public List<String> kafkaBootstrapServers;

	@JsonCreator
	public ClusterSettings(@JsonProperty(required = true, value = "id") String id,
			@JsonProperty(required = true, value = "kafkaBootstrapServers") List<String> kafkaBootstrapServers) {
		this.id = id;
		this.kafkaBootstrapServers = kafkaBootstrapServers;
	}
}
