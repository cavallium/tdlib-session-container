package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Define the cluster structure
 */
public class ClusterSettings {

	public String id;
	public List<NodeSettings> nodes;

	@JsonCreator
	public ClusterSettings(@JsonProperty(required = true, value = "id") String id,
			@JsonProperty(required = true, value = "nodes") List<NodeSettings> nodes) {
		this.id = id;
		this.nodes = nodes;
	}
}
