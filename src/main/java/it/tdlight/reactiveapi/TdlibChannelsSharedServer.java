package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.reactiveapi.Event.ClientBoundEvent;
import it.tdlight.reactiveapi.Event.OnRequest;
import it.tdlight.reactiveapi.Event.OnResponse;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class TdlibChannelsSharedServer implements Closeable {

	private final TdlibChannelsServers tdServersChannels;
	private final Disposable responsesSub;
	private final AtomicReference<Disposable> requestsSub = new AtomicReference<>();
	private final Many<OnResponse<TdApi.Object>> responses = Sinks.many().unicast().onBackpressureBuffer(
			Queues.<OnResponse<TdApi.Object>>get(65535).get());
	private final Flux<Timestamped<OnRequest<Object>>> requests;

	public TdlibChannelsSharedServer(TdlibChannelsServers tdServersChannels) {
		this.tdServersChannels = tdServersChannels;
		this.responsesSub = tdServersChannels.response()
				.sendMessages(responses.asFlux().log("responses", Level.FINEST, SignalType.ON_NEXT))
				.subscribeOn(Schedulers.parallel())
				.subscribe();
		this.requests = tdServersChannels.request().consumeMessages();
	}

	public Flux<Timestamped<OnRequest<Object>>> requests() {
		return requests
				//.onBackpressureBuffer(8192, BufferOverflowStrategy.DROP_OLDEST)
				.log("requests", Level.FINEST, SignalType.REQUEST, SignalType.ON_NEXT);
	}

	public Disposable events(String lane, Flux<ClientBoundEvent> eventFlux) {
		return tdServersChannels.events(lane)
				.sendMessages(eventFlux)
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	public Many<OnResponse<TdApi.Object>> responses() {
		return responses;
	}

	@Override
	public void close() {
		responsesSub.dispose();
		var requestsSub = this.requestsSub.get();
		if (requestsSub != null) {
			requestsSub.dispose();
		}
		tdServersChannels.close();
	}
}
