package it.tdlight.reactiveapi;

import io.atomix.core.Atomix;
import it.tdlight.jni.TdApi;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.SerializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Schedulers;

public class ReactiveApiPublisher {

	private static final SchedulerExecutor SCHEDULER_EXECUTOR = new SchedulerExecutor(Schedulers.boundedElastic());

	private final Atomix atomix;
	private final long userId;
	private final long sessionId;
	private final String botToken;
	private final Long phoneNumber;

	private ReactiveApiPublisher(Atomix atomix, long sessionId, long userId, String botToken, Long phoneNumber) {
		this.atomix = atomix;
		this.userId = userId;
		this.sessionId = sessionId;
		this.botToken = botToken;
		this.phoneNumber = phoneNumber;
	}

	public static ReactiveApiPublisher fromToken(Atomix atomix, Long sessionId, long userId, String token) {
		return new ReactiveApiPublisher(atomix, sessionId, userId, token, null);
	}

	public static ReactiveApiPublisher fromPhoneNumber(Atomix atomix, Long sessionId, long userId, long phoneNumber) {
		return new ReactiveApiPublisher(atomix, sessionId, userId, null, phoneNumber);
	}

	public void start(Path path) {

	}
}
