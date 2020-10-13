package it.tdlight.tdlibsession.td.middle.server;

import io.vertx.core.Promise;
import it.tdlight.jni.TdApi.Object;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Async.Execute;
import org.jetbrains.annotations.Async.Schedule;

public class RequestIdToReplyAddress {
	private final ConcurrentHashMap<Long, Promise<Object>> reqIdToReplyAddress = new ConcurrentHashMap<>();;

	public RequestIdToReplyAddress() {

	}

	public void schedule(@Schedule Long requestId, Promise<Object> replyPromise) {
		reqIdToReplyAddress.put(requestId, replyPromise);
	}

	public void failed(@Execute Long requestId, Promise<Object> replyPromise) {
		reqIdToReplyAddress.remove(requestId, replyPromise);
	}

	public void complete(@Execute Long id, Object item) {
		var replyPromise = reqIdToReplyAddress.remove(id);
		Objects.requireNonNull(replyPromise, () -> "Reply promise must be not empty");
		replyPromise.complete(item);
	}
}
