package it.tdlight.reactiveapi;

import com.google.common.net.HostAndPort;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

public final class RSocketParameters implements ChannelsParameters {

	private final boolean client;
	private final TransportFactory transportFactory;
	private final List<String> lanes;

	public RSocketParameters(boolean client, String host, List<String> lanes) {
		this.client = client;
		var hostAndPort = HostAndPort.fromString(host);
		this.transportFactory = TransportFactory.tcp(hostAndPort);
		this.lanes = lanes;
	}

	public RSocketParameters(boolean client, TransportFactory transportFactory, List<String> lanes) {
		this.client = client;
		this.transportFactory = transportFactory;
		this.lanes = lanes;
	}

	@Override
	public Set<String> getAllLanes() {
		var lanes = new LinkedHashSet<String>(this.lanes.size() + 1);
		lanes.add("main");
		lanes.addAll(this.lanes);
		return lanes;
	}

	public boolean isClient() {
		return client;
	}

	public TransportFactory transportFactory() {
		return transportFactory;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RSocketParameters that = (RSocketParameters) o;
		return client == that.client && Objects.equals(transportFactory, that.transportFactory) && Objects.equals(lanes,
				that.lanes
		);
	}

	@Override
	public int hashCode() {
		return Objects.hash(client, transportFactory, lanes);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RSocketParameters.class.getSimpleName() + "[", "]")
				.add("client=" + client)
				.add("transportFactory=" + transportFactory)
				.add("lanes=" + lanes)
				.toString();
	}
}
