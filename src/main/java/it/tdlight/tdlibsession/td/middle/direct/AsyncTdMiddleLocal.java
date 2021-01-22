package it.tdlight.tdlibsession.td.middle.direct;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.tdlibsession.td.middle.TdClusterManager;
import it.tdlight.tdlibsession.td.middle.client.AsyncTdMiddleEventBusClient;
import it.tdlight.tdlibsession.td.middle.server.AsyncTdMiddleEventBusServer;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;

public class AsyncTdMiddleLocal implements AsyncTdMiddle {

	private final AsyncTdMiddleEventBusServer srv;
	private final TdClusterManager masterClusterManager;
	private final One<AsyncTdMiddle> cli = Sinks.one();
	private final String botAlias;
	private final String botAddress;

	public AsyncTdMiddleLocal(TdClusterManager masterClusterManager, String botAlias, String botAddress) {
		this.srv = new AsyncTdMiddleEventBusServer(masterClusterManager);
		this.masterClusterManager = masterClusterManager;
		this.botAlias = botAlias;
		this.botAddress = botAddress;
	}

	public Mono<AsyncTdMiddleLocal> start() {
		return srv
				.start(botAddress, botAlias, true)
				.onErrorMap(InitializationException::new)
				.single()
				.then(AsyncTdMiddleEventBusClient.getAndDeployInstance(masterClusterManager, botAlias, botAddress, true))
				.single()
				.doOnNext(this.cli::tryEmitValue)
				.doOnError(this.cli::tryEmitError)
				.thenReturn(this);
	}

	@Override
	public Flux<TdApi.Object> receive() {
		return cli.asMono().single().flatMapMany(AsyncTdMiddle::receive);
	}

	@Override
	public <T extends Object> Mono<TdResult<T>> execute(Function request, boolean executeDirectly) {
		return cli.asMono().single().flatMap(c -> c.execute(request, executeDirectly));
	}
}
