package it.tdlight.tdlibsession.remoteclient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoteClientBotAddresses {

	private final LinkedHashSet<String> addresses;
	private final LinkedHashSet<String> tempAddresses;
	private final Path addressesFilePath;

	public RemoteClientBotAddresses(Path addressesFilePath) throws IOException {
		this.addressesFilePath = addressesFilePath;
		if (Files.notExists(addressesFilePath)) {
			Files.createFile(addressesFilePath);
		}
		tempAddresses = new LinkedHashSet<>();
		addresses = Files
				.readAllLines(addressesFilePath, StandardCharsets.UTF_8)
				.stream()
				.filter(address -> !address.isBlank())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	public synchronized void putAddress(String address) throws IOException {
		tempAddresses.remove(address);
		addresses.add(address);
		Files.write(addressesFilePath, addresses, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
	}

	public synchronized void putTempAddress(String address) {
		tempAddresses.add(address);
	}

	public synchronized void removeAddress(String address) throws IOException {
		tempAddresses.remove(address);
		addresses.remove(address);
		Files.write(addressesFilePath, addresses, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
	}

	public synchronized boolean has(String botAddress) {
		return addresses.contains(botAddress) || tempAddresses.contains(botAddress);
	}

	public synchronized Set<String> values() {
		return new HashSet<>(addresses);
	}
}
