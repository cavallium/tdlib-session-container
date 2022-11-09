package it.tdlight.reactiveapi;

import com.google.common.net.HostAndPort;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;

public interface TransportFactory {

	ClientTransport getClientTransport(int index);

	ServerTransport<?> getServerTransport(int index);

	static TransportFactory tcp(HostAndPort baseHost) {
		return new TransportFactoryTcp(baseHost);
	}

	static TransportFactory local(String prefix) {
		return new TransportFactoryLocal(prefix);
	}
}
