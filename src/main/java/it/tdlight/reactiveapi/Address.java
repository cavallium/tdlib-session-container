package it.tdlight.reactiveapi;

import java.util.Arrays;

public record Address(String host, int port) {
	public Address {
		if (host.isBlank()) {
			throw new IllegalArgumentException("Host is blank");
		}
		if (port < 0) {
			throw new IndexOutOfBoundsException(port);
		}
		if (port >= 65536) {
			throw new IndexOutOfBoundsException(port);
		}
	}

	public static Address fromString(String address) {
		var parts = address.split(":");
		if (parts.length < 2) {
			throw new IllegalArgumentException("Malformed client address, it must have a port (host:port)");
		}
		var host = String.join(":", Arrays.copyOf(parts, parts.length - 1));
		var port = Integer.parseUnsignedInt(parts[parts.length - 1]);
		return new Address(host, port);
	}
}
