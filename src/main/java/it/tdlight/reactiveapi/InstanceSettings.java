package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class InstanceSettings {

	@NotNull
	public String id;

	/**
	 * True if this is just a client, false if this is a complete node
	 * <p>
	 * A client is a lightweight node
	 */
	public boolean client;

	/**
	 * If {@link #client} is true, this will be the address of this client
	 */
	public @Nullable String clientAddress;

	@JsonCreator
	public InstanceSettings(@JsonProperty(required = true, value = "id") @NotNull String id,
			@JsonProperty(required = true, value = "client") boolean client,
			@JsonProperty("clientAddress") @Nullable String clientAddress) {
		this.id = id;
		this.client = client;
		this.clientAddress = clientAddress;
	}
}
