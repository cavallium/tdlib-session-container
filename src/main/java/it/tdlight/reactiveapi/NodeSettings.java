package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import org.jetbrains.annotations.NotNull;

public class NodeSettings {

	public @NotNull String id;
	public @NotNull String address;

	public NodeSettings(@JsonProperty(required = true, value = "id") @NotNull String id,
			@JsonProperty(required = true, value = "address") @NotNull String address) {
		this.id = id;
		this.address = address;
	}
}
