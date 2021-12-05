package it.tdlight.reactiveapi;

import io.atomix.core.Atomix;
import java.nio.file.Path;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

public class ReactiveApiPublisher {


	private static final Logger LOG = LoggerFactory.getLogger(ReactiveApiPublisher.class);
	private static final SchedulerExecutor SCHEDULER_EXECUTOR = new SchedulerExecutor(Schedulers.boundedElastic());

	private final Atomix atomix;
	private final long userId;
	private final long liveId;
	private final String botToken;
	private final Long phoneNumber;

	private ReactiveApiPublisher(Atomix atomix, long liveId, long userId, String botToken, Long phoneNumber) {
		this.atomix = atomix;
		this.userId = userId;
		this.liveId = liveId;
		this.botToken = botToken;
		this.phoneNumber = phoneNumber;
	}

	public static ReactiveApiPublisher fromToken(Atomix atomix, Long liveId, long userId, String token) {
		return new ReactiveApiPublisher(atomix, liveId, userId, token, null);
	}

	public static ReactiveApiPublisher fromPhoneNumber(Atomix atomix, Long liveId, long userId, long phoneNumber) {
		return new ReactiveApiPublisher(atomix, liveId, userId, null, phoneNumber);
	}

	public void start(Path path) {
		LOG.info("Starting session \"{}\" in path \"{}\"", this, path);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ReactiveApiPublisher.class.getSimpleName() + "[", "]")
				.add("userId=" + userId)
				.add("liveId=" + liveId)
				.add("botToken='" + botToken + "'")
				.add("phoneNumber=" + phoneNumber)
				.toString();
	}
}
