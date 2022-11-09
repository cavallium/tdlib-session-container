package it.tdlight.reactiveapi;

import com.google.common.net.HostAndPort;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.util.Objects;
import java.util.StringJoiner;

class TransportFactoryTcp implements TransportFactory {

	private final HostAndPort baseHost;

	TransportFactoryTcp(HostAndPort baseHost) {
		this.baseHost = baseHost;
	}

	@Override
	public ClientTransport getClientTransport(int index) {
		return TcpClientTransport.create(baseHost.getHost(), getPort(index));
	}

	@Override
	public ServerTransport<?> getServerTransport(int index) {
		return TcpServerTransport.create(baseHost.getHost(), getPort(index));
	}

	private int getPort(int index) {
		return baseHost.getPort() + index;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TransportFactoryTcp that = (TransportFactoryTcp) o;
		return Objects.equals(baseHost, that.baseHost);
	}

	@Override
	public int hashCode() {
		return Objects.hash(baseHost);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TransportFactoryTcp.class.getSimpleName() + "[", "]")
				.add("baseHost=" + baseHost)
				.toString();
	}
}
