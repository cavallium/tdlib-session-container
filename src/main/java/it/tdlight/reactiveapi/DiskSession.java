package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Lanes.MAIN_LANE;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

@JsonInclude(Include.NON_NULL)
public class DiskSession {

	@Nullable
	public String token;
	@Nullable
	public Long phoneNumber;
	@Nullable
	public String lane;

	@JsonCreator
	public DiskSession(@JsonProperty("token") @Nullable String token,
			@JsonProperty("phoneNumber") @Nullable Long phoneNumber,
			@JsonProperty("lane") @Nullable String lane) {
		this.token = token;
		this.phoneNumber = phoneNumber;
		this.lane = Objects.requireNonNullElse(lane, MAIN_LANE);
		this.validate();
	}

	public void validate() {
		if ((token == null) == (phoneNumber == null)) {
			throw new UnsupportedOperationException("You must set either a token or a phone number");
		}
	}
}
