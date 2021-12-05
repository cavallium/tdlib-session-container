package it.tdlight.reactiveapi;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.LoadSessionFromDiskRequest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.lang3.SerializationException;

public sealed interface CreateSessionRequest permits CreateUserSessionRequest, CreateBotSessionRequest,
		LoadSessionFromDiskRequest {

	long userId();

	static CreateSessionRequest deserializeBytes(byte[] bytes) {
		byte type = bytes[0];
		long userId = Longs.fromBytes(bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8]);
		return switch (type) {
			case 0 -> new CreateUserSessionRequest(userId,
					Longs.fromBytes(bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16])
			);
			case 1 -> new CreateBotSessionRequest(userId,
					new String(bytes, 1 + Long.BYTES + Integer.BYTES, Ints.fromBytes(bytes[9], bytes[10], bytes[11], bytes[12]))
			);
			case 2 -> {
				var dis = new DataInputStream(new ByteArrayInputStream(bytes, 1 + Long.BYTES, bytes.length - (1 + Long.BYTES)));
				try {
					var isBot = dis.readBoolean();
					String token;
					Long phoneNumber;
					if (isBot) {
						token = dis.readUTF();
						phoneNumber = null;
					} else {
						token = null;
						phoneNumber = dis.readLong();
					}
					yield new LoadSessionFromDiskRequest(userId, token, phoneNumber, dis.readBoolean());
				} catch (IOException e) {
					throw new SerializationException(e);
				}
			}
			default -> throw new IllegalStateException("Unexpected value: " + type);
		};
	}

	final record CreateUserSessionRequest(long userId, long phoneNumber) implements CreateSessionRequest {}

	final record CreateBotSessionRequest(long userId, String token) implements CreateSessionRequest {}

	final record LoadSessionFromDiskRequest(long userId, String token, Long phoneNumber, boolean createNew) implements
			CreateSessionRequest {

		public LoadSessionFromDiskRequest {
			if ((token == null) == (phoneNumber == null)) {
				throw new IllegalArgumentException("This must be either a bot or an user");
			}
		}
	}
}
