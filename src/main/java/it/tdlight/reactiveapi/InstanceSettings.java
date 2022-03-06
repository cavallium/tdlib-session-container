package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
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

	/**
	 * If {@link #client} is false, this will transform resulting events <b>before</b> being sent
	 */
	public @Nullable List<Class<? extends ResultingEventTransformer>> resultingEventTransformers;

	@JsonCreator
	public InstanceSettings(@JsonProperty(required = true, value = "id") @NotNull String id,
			@JsonProperty(required = true, value = "client") boolean client,
			@JsonProperty("clientAddress") @Nullable String clientAddress,
			@JsonProperty("resultingEventTransformers") @Nullable
					List<Class<? extends ResultingEventTransformer>> resultingEventTransformers) {
		this.id = id;
		this.client = client;
		this.clientAddress = clientAddress;
		this.resultingEventTransformers = resultingEventTransformers;
	}
}
