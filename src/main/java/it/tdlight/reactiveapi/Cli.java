package it.tdlight.reactiveapi;

import static it.tdlight.reactiveapi.Lanes.MAIN_LANE;
import static java.util.Collections.unmodifiableSet;

import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import net.minecrell.terminalconsole.SimpleTerminalConsole;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import reactor.core.scheduler.Schedulers;

public class Cli {

	private static final Logger LOG = LogManager.getLogger(Cli.class);

	private static final Object parameterLock = new Object();
	private static boolean askedParameter = false;
	private static CompletableFuture<String> askedParameterResult = null;

	public static void main(String[] args) throws IOException {
		var validArgs = Entrypoint.parseArguments(args);
		var api = (AtomixReactiveApi) Entrypoint.start(validArgs);

		AtomicBoolean alreadyShutDown = new AtomicBoolean(false);
		AtomicBoolean acceptInputs = new AtomicBoolean(true);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			acceptInputs.set(false);
			if (alreadyShutDown.compareAndSet(false, true)) {
				api.close().subscribeOn(Schedulers.immediate()).subscribe();
			}
		}));

		var console = new SimpleTerminalConsole() {

			private static final Set<String> commands = Set.of("exit",
					"stop",
					"createsession",
					"help",
					"man",
					"?",
					"sessions",
					"localsessions"
			);

			@Override
			protected LineReader buildReader(LineReaderBuilder builder) {
				return super.buildReader(builder);
			}

			@Override
			protected boolean isRunning() {
				return acceptInputs.get();
			}

			@Override
			protected void runCommand(String command) {
				synchronized (parameterLock) {
					if (askedParameter) {
						askedParameterResult.complete(command);
						askedParameterResult = null;
						askedParameter = false;
						return;
					}
				}

				var parts = command.split(" ", 2);
				var commandName = parts[0].trim().toLowerCase();
				String commandArgs;
				if (parts.length > 1) {
					commandArgs = parts[1].trim();
				} else {
					commandArgs = "";
				}
				switch (commandName) {
					case "exit", "stop" -> shutdown();
					case "createsession" -> createSession(api, commandArgs);
					case "help", "?", "man" -> LOG.info("Commands: {}", commands);
					case "sessions" -> printSessions(api, false);
					case "localsessions" -> printSessions(api, true);
					default -> LOG.info("Unknown command \"{}\"", command);
				}
			}

			private void printSessions(ReactiveApi api, boolean onlyLocal) {
				LOG.info("Not implemented");
			}

			@Override
			protected void shutdown() {
				acceptInputs.set(false);
				if (alreadyShutDown.compareAndSet(false, true)) {
					Runtime.getRuntime().exit(0);
				}
			}
		};
		console.start();
		api.waitForExit();
	}

	private static void createSession(ReactiveApi api, String commandArgs) {
		var parts = commandArgs.split(" ");
		boolean invalid = false;
		if (parts.length == 4 || parts.length == 3) {
			String lane;
			if (parts.length == 4) {
				lane = parts[3];
			} else {
				lane = MAIN_LANE;
			}
			CreateSessionRequest request = switch (parts[0]) {
				case "bot" -> new CreateBotSessionRequest(Long.parseLong(parts[1]), parts[2], lane);
				case "user" -> new CreateUserSessionRequest(Long.parseLong(parts[1]),
						Long.parseLong(parts[2]), lane);
				default -> {
					invalid = true;
					yield null;
				}
			};
			if (!invalid) {
				api
						.createSession(request)
						.doOnNext(response -> LOG.info("Created a session with live id \"{}\"", response.sessionId()))
						.block();
			}
		} else {
			invalid = true;
		}
		if (invalid) {
			LOG.error("Syntax: CreateSession <\"bot\"|\"user\"> <userid> <token|phoneNumber> [lane]");
		}
	}

	public static String askParameter(String question) {
		var cf = new CompletableFuture<String>();
		synchronized (parameterLock) {
			LOG.info(question);
			askedParameter = true;
			askedParameterResult = cf;
		}
		return cf.join();
	}
}
