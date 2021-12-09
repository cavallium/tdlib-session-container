package it.tdlight.reactiveapi;

import io.atomix.core.Atomix;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateBotSessionRequest;
import it.tdlight.reactiveapi.CreateSessionRequest.CreateUserSessionRequest;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import net.minecrell.terminalconsole.SimpleTerminalConsole;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cli {

	private static final Logger LOG = LoggerFactory.getLogger(Cli.class);

	public static void main(String[] args) throws IOException {
		var validArgs = Entrypoint.parseArguments(args);
		var atomixBuilder = Atomix.builder();
		var api = Entrypoint.start(validArgs, atomixBuilder);

		AtomicBoolean alreadyShutDown = new AtomicBoolean(false);
		AtomicBoolean acceptInputs = new AtomicBoolean(true);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			acceptInputs.set(false);
			if (alreadyShutDown.compareAndSet(false, true)) {
				api.getAtomix().stop().join();
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
				api.getAllUsers().subscribe(sessions -> {
					StringBuilder sb = new StringBuilder();
					sb.append("Sessions:\n");
					for (var userEntry : sessions.entrySet()) {
						var userId = userEntry.getKey();
						var nodeId = userEntry.getValue();
						if (!onlyLocal || api.is(nodeId)) {
							sb.append(" - session #IDU").append(userId);
							if (!onlyLocal) {
								sb.append(": ").append(nodeId);
							}
							sb.append("\n");
						}
					}
					LOG.info(sb.toString());
				});
			}

			@Override
			protected void shutdown() {
				acceptInputs.set(false);
				if (alreadyShutDown.compareAndSet(false, true)) {
					api.getAtomix().stop().join();
					System.exit(0);
				}
			}
		};
		console.start();
	}

	private static void createSession(ReactiveApi api, String commandArgs) {
		var parts = commandArgs.split(" ");
		boolean invalid = false;
		if (parts.length == 3) {
			CreateSessionRequest request = switch (parts[0]) {
				case "bot" -> new CreateBotSessionRequest(Long.parseLong(parts[1]), parts[2]);
				case "user" -> new CreateUserSessionRequest(Long.parseLong(parts[1]),
						Long.parseLong(parts[2]));
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
			LOG.error("Syntax: CreateSession <\"bot\"|\"user\"> <userid> <token|phoneNumber>");
		}
	}
}
