package it.tdlight.tdlibsession.td.easy;

import it.tdlight.jni.TdApi.AuthenticationCodeType;
import java.util.StringJoiner;

public class ParameterInfoCode implements ParameterInfo {
	private final String phoneNumber;
	private final AuthenticationCodeType nextType;
	private final int timeout;
	private final AuthenticationCodeType type;

	public ParameterInfoCode(String phoneNumber,
			AuthenticationCodeType nextType,
			int timeout,
			AuthenticationCodeType type) {
		this.phoneNumber = phoneNumber;
		this.nextType = nextType;
		this.timeout = timeout;
		this.type = type;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public AuthenticationCodeType getNextType() {
		return nextType;
	}

	public int getTimeout() {
		return timeout;
	}

	public AuthenticationCodeType getType() {
		return type;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ParameterInfoCode.class.getSimpleName() + "[", "]")
				.add("phoneNumber='" + phoneNumber + "'")
				.add("nextType=" + nextType)
				.add("timeout=" + timeout)
				.add("type=" + type)
				.toString();
	}
}
