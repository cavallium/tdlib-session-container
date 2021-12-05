package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class DiskSessions {

	@NotNull
	public String path;

	/**
	 * key: session folder name
	 */
	@NotNull
	private Map<Long, DiskSession> sessions;

	@JsonCreator
	public DiskSessions(@JsonProperty(required = true, value = "path") @NotNull String path,
			@JsonProperty(required = true, value = "sessions") @NotNull Map<Long, DiskSession> userIdToSession) {
		this.path = path;
		this.sessions = userIdToSession;
	}

	public Map<Long, DiskSession> userIdToSession() {
		return sessions;
	}
}
