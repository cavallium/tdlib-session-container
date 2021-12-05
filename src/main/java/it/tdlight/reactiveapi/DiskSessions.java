package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class DiskSessions {

	@NotNull
	public String path;

	/**
	 * key: session folder name
	 */
	@NotNull
	public Map<String, DiskSession> sessions;

	@JsonCreator
	public DiskSessions(@JsonProperty(required = true, value = "path") @NotNull String path,
			@JsonProperty(required = true, value = "sessions") @NotNull Map<String, DiskSession> sessions) {
		this.path = path;
		this.sessions = sessions;
	}
}
