package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class InstanceSettings {

	@NotNull
	public String id;

	public InstanceType instanceType;

	/**
	 * If {@link #instanceType} is true, this will be the address of this client
	 */
	public @Nullable String listenAddress;

	/**
	 * If {@link #instanceType} is false, this will transform resulting events <b>before</b> being sent
	 */
	public @Nullable List<Class<? extends ResultingEventTransformer>> resultingEventTransformers;

	public InstanceSettings(@NotNull String id,
			@NotNull InstanceType instanceType,
			@Nullable String listenAddress,
			@Nullable List<Class<? extends ResultingEventTransformer>> resultingEventTransformers) {
		this.id = id;
		this.instanceType = instanceType;
		this.listenAddress = listenAddress;
		this.resultingEventTransformers = resultingEventTransformers;
	}

	@JsonCreator
	public InstanceSettings(@JsonProperty(required = true, value = "id") @NotNull String id,
			@Deprecated @JsonProperty(value = "client", defaultValue = "null") Boolean deprecatedIsClient,
			@JsonProperty(value = "instanceType", defaultValue = "null") String instanceType,
			@Deprecated @JsonProperty(value = "clientAddress", defaultValue = "null") @Nullable String deprecatedClientAddress,
			@JsonProperty(value = "listenAddress", defaultValue = "null") @Nullable String listenAddress,
			@JsonProperty("resultingEventTransformers")
			@Nullable List<Class<? extends ResultingEventTransformer>> resultingEventTransformers) {
		this.id = id;
		if (deprecatedIsClient != null) {
			this.instanceType = deprecatedIsClient ? InstanceType.UPDATES_CONSUMER : InstanceType.TDLIB;
		} else {
			this.instanceType = InstanceType.valueOf(instanceType.toUpperCase());
		}
		if (deprecatedClientAddress != null) {
			this.listenAddress = deprecatedClientAddress;
		} else {
			this.listenAddress = listenAddress;
		}
		this.resultingEventTransformers = resultingEventTransformers;
	}
}
