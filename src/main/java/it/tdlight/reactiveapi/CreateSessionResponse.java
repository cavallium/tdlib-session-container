package it.tdlight.reactiveapi;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public record CreateSessionResponse(long sessionId) {

	public static byte[] serializeBytes(CreateSessionResponse createSessionResponse) {
		return Longs.toByteArray(createSessionResponse.sessionId);
	}
}
