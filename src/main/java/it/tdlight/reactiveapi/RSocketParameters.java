package it.tdlight.reactiveapi;

import com.google.common.net.HostAndPort;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

public final class RSocketParameters implements ChannelsParameters {

	private final boolean client;
	private final HostAndPort host;
	private final List<String> lanes;

	public RSocketParameters(boolean client, String host, List<String> lanes) {
		this.client = client;
		this.host = HostAndPort.fromString(host);
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

	public HostAndPort baseHost() {
		return host;
	}

	public HostAndPort channelHost(String channelName) {
		return switch (channelName) {
			case "request" -> HostAndPort.fromParts(host.getHost(), host.getPort());
			case "response" -> HostAndPort.fromParts(host.getHost(), host.getPort() + 1);
			default -> {
				if (channelName.startsWith("event-")) {
					var lane = channelName.substring("event-".length());
					int index;
					if (lane.equals("main")) {
						index = 0;
					} else {
						index = lanes.indexOf(lane);
						if (index < 0) {
							throw new IllegalArgumentException("Unknown lane: " + lane);
						}
						index++;
					}
					yield HostAndPort.fromParts(host.getHost(), host.getPort() + 2 + index);
				} else {
					throw new IllegalArgumentException("Unknown channel: " + channelName);
				}
			}
		};
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		var that = (RSocketParameters) obj;
		return Objects.equals(this.client, that.client) && Objects.equals(
				this.host,
				that.host) && Objects.equals(this.lanes, that.lanes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(client, host, lanes);
	}

	@Override
	public String toString() {
		return "RSocketParameters[client=" + client + ", " + "host=" + host + ", "
				+ "lanes=" + lanes + ']';
	}

}
