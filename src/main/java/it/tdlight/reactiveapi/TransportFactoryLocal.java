package it.tdlight.reactiveapi;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import java.util.Objects;
import java.util.StringJoiner;

class TransportFactoryLocal implements TransportFactory {

	private final String prefix;

	TransportFactoryLocal(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public ClientTransport getClientTransport(int index) {
		return LocalClientTransport.create(getLabel(index));
	}

	@Override
	public ServerTransport<?> getServerTransport(int index) {
		return LocalServerTransport.create(getLabel(index));
	}

	private String getLabel(int index) {
		return prefix + "-" + index;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TransportFactoryLocal that = (TransportFactoryLocal) o;
		return Objects.equals(prefix, that.prefix);
	}

	@Override
	public int hashCode() {
		return Objects.hash(prefix);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", TransportFactoryLocal.class.getSimpleName() + "[", "]")
				.add("prefix='" + prefix + "'")
				.toString();
	}
}
