package it.tdlight.reactiveapi;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;

public sealed interface CreateSessionRequest permits CreateUserSessionRequest, CreateBotSessionRequest,
		LoadSessionFromDiskRequest {

	long userId();

	record CreateUserSessionRequest(long userId, long phoneNumber) implements CreateSessionRequest {}

	record CreateBotSessionRequest(long userId, String token) implements CreateSessionRequest {}

	record LoadSessionFromDiskRequest(long userId, String token, Long phoneNumber, boolean createNew) implements
			CreateSessionRequest {

		public LoadSessionFromDiskRequest {
			if ((token == null) == (phoneNumber == null)) {
				throw new IllegalArgumentException("This must be either a bot or an user");
			}
		}
	}
}
