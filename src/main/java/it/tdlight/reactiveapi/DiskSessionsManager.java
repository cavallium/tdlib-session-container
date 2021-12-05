package it.tdlight.reactiveapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class DiskSessionsManager {

	private final ObjectMapper mapper;
	private final DiskSessions diskSessionsSettings;
	private final File file;

	public DiskSessionsManager(ObjectMapper mapper, String diskSessionsConfigPath) throws IOException {
		this.mapper = mapper;
		this.file = Paths.get(diskSessionsConfigPath).toFile();
		diskSessionsSettings = mapper.readValue(file, DiskSessions.class);
	}

	public DiskSessions getSettings() {
		return diskSessionsSettings;
	}

	public synchronized void save() throws IOException {
		mapper.writeValue(file, diskSessionsSettings);
	}
}
