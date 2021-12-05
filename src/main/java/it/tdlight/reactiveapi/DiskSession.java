package it.tdlight.reactiveapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

@JsonInclude(Include.NON_NULL)
public class DiskSession {

	public long userId;
	@Nullable
	public String token;
	@Nullable
	public Long phoneNumber;

	@JsonCreator
	public DiskSession(@JsonProperty(required = true, value = "userId") long userId,
			@JsonProperty("token") @Nullable String token,
			@JsonProperty("phoneNumber") @Nullable Long phoneNumber) {
		this.userId = userId;
		this.token = token;
		this.phoneNumber = phoneNumber;
		this.validate();
	}

	public void validate() {
		if ((token == null) == (phoneNumber == null)) {
			throw new UnsupportedOperationException("You must set either a token or a phone number");
		}
	}
}
