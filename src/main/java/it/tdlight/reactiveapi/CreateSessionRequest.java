package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;

public sealed interface CreateSessionRequest permits CreateUserSessionRequest, CreateBotSessionRequest,
		LoadSessionFromDiskRequest {

	long userId();

	record CreateUserSessionRequest(long userId, long phoneNumber, String lane) implements CreateSessionRequest {}

	record CreateBotSessionRequest(long userId, String token, String lane) implements CreateSessionRequest {}

	record LoadSessionFromDiskRequest(long userId, String token, Long phoneNumber, String lane,
																		boolean createNew) implements CreateSessionRequest {

		public LoadSessionFromDiskRequest {
			if ((token == null) == (phoneNumber == null)) {
				throw new IllegalArgumentException("This must be either a bot or an user");
			}
		}
	}
}
