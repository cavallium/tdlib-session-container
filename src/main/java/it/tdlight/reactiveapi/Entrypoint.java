package it.tdlight.reactiveapi;

import static java.util.Collections.unmodifiableSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import it.tdlight.reactiveapi.AtomixReactiveApi.AtomixReactiveApiMode;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Entrypoint {

	private static final Logger LOG = LoggerFactory.getLogger(Entrypoint.class);

	public record ValidEntrypointArgs(String clusterPath, String instancePath, String diskSessionsPath) {}

	public static ValidEntrypointArgs parseArguments(String[] args) {
		// Check arguments validity
		if (args.length != 3
				|| args[0].isBlank()
				|| args[1].isBlank()
				|| args[2].isBlank()
				|| !Files.isRegularFile(Paths.get(args[0]))
				|| !Files.isRegularFile(Paths.get(args[1]))
				|| !Files.isRegularFile(Paths.get(args[2]))) {
			System.err.println("Syntax: executable <path/to/cluster.yaml> <path/to/instance.yaml> <path/to/disk-sessions.yaml>");
			System.exit(1);
		}
		return new ValidEntrypointArgs(args[0], args[1], args[2]);
	}

	public static ReactiveApi start(ValidEntrypointArgs args) throws IOException {
		// Read settings
		ClusterSettings clusterSettings;
		InstanceSettings instanceSettings;
		DiskSessionsManager diskSessions;
		{
			var mapper = new ObjectMapper(new YAMLFactory());
			mapper.findAndRegisterModules();
			String clusterConfigPath = args.clusterPath;
			String instanceConfigPath = args.instancePath;
			String diskSessionsConfigPath = args.diskSessionsPath;
			clusterSettings = mapper.readValue(Paths.get(clusterConfigPath).toFile(), ClusterSettings.class);
			instanceSettings = mapper.readValue(Paths.get(instanceConfigPath).toFile(), InstanceSettings.class);
			if (instanceSettings.client) {
				diskSessions = null;
			} else {
				diskSessions = new DiskSessionsManager(mapper, diskSessionsConfigPath);
			}
		}

		return start(clusterSettings, instanceSettings, diskSessions);
	}

	public static ReactiveApi start(ClusterSettings clusterSettings,
			InstanceSettings instanceSettings,
			@Nullable DiskSessionsManager diskSessions) {

		Set<ResultingEventTransformer> resultingEventTransformerSet;
		AtomixReactiveApiMode mode = AtomixReactiveApiMode.SERVER;
		if (instanceSettings.client) {
			if (diskSessions != null) {
				throw new IllegalArgumentException("A client instance can't have a session manager!");
			}
			if (instanceSettings.clientAddress == null) {
				throw new IllegalArgumentException("A client instance must have an address (host:port)");
			}
			mode = AtomixReactiveApiMode.CLIENT;
			resultingEventTransformerSet = Set.of();
		} else {
			if (diskSessions == null) {
				throw new IllegalArgumentException("A full instance must have a session manager!");
			}

			resultingEventTransformerSet = new HashSet<>();
			if (instanceSettings.resultingEventTransformers != null) {
				for (var resultingEventTransformer: instanceSettings.resultingEventTransformers) {
					try {
						var instance = resultingEventTransformer.getConstructor().newInstance();
						resultingEventTransformerSet.add(instance);
						LOG.info("Loaded and applied resulting event transformer: " + resultingEventTransformer.getName());
					} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
						throw new IllegalArgumentException("Failed to load resulting event transformer: "
								+ resultingEventTransformer.getName());
					} catch (NoSuchMethodException e) {
						throw new IllegalArgumentException("The client transformer must declare an empty constructor: "
								+ resultingEventTransformer.getName());
					}
				}
			}

			resultingEventTransformerSet = unmodifiableSet(resultingEventTransformerSet);
		}

		var kafkaParameters = new KafkaParameters(clusterSettings, instanceSettings.id);

		var api = new AtomixReactiveApi(mode, kafkaParameters, diskSessions, resultingEventTransformerSet);

		LOG.info("Starting ReactiveApi...");

		api.start().block();

		LOG.info("Started ReactiveApi");

		return api;
	}

	public static void main(String[] args) throws IOException {
		var validArgs = parseArguments(args);
		var api = start(validArgs);
		api.waitForExit();
	}
}
