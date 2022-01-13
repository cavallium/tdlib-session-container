package it.tdlight.reactiveapi;

import static java.util.Collections.unmodifiableSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.profile.ConsensusProfileConfig;
import io.atomix.core.profile.Profile;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
			System.err.println("Syntax: executable <path/to/cluster.yaml> <path/to/instance.yaml> <path/to/disk-sessions.yaml");
			System.exit(1);
		}
		return new ValidEntrypointArgs(args[0], args[1], args[2]);
	}

	public static ReactiveApi start(ValidEntrypointArgs args, AtomixBuilder atomixBuilder) throws IOException {
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

		return start(clusterSettings, instanceSettings, diskSessions, atomixBuilder);
	}

	public static ReactiveApi start(ClusterSettings clusterSettings,
			InstanceSettings instanceSettings,
			@Nullable DiskSessionsManager diskSessions,
			AtomixBuilder atomixBuilder) {

		atomixBuilder.withCompatibleSerialization(false);
		atomixBuilder.withClusterId(clusterSettings.id);

		Set<ResultingEventTransformer> resultingEventTransformerSet;
		String nodeId;
		if (instanceSettings.client) {
			if (diskSessions != null) {
				throw new IllegalArgumentException("A client instance can't have a session manager!");
			}
			if (instanceSettings.clientAddress == null) {
				throw new IllegalArgumentException("A client instance must have an address (host:port)");
			}
			var address = Address.fromString(instanceSettings.clientAddress);
			atomixBuilder.withMemberId(instanceSettings.id).withHost(address.host()).withPort(address.port());
			nodeId = null;
			resultingEventTransformerSet = Set.of();
		} else {
			if (diskSessions == null) {
				throw new IllegalArgumentException("A full instance must have a session manager!");
			}
			// Find node settings
			var nodeSettingsOptional = clusterSettings.nodes
					.stream()
					.filter(node -> node.id.equals(instanceSettings.id))
					.findAny();

			// Check node settings presence
			if (nodeSettingsOptional.isEmpty()) {
				System.err.printf("Node id \"%s\" has not been described in cluster.yaml nodes list%n", instanceSettings.id);
				System.exit(2);
			}

			var nodeSettings = nodeSettingsOptional.get();

			var address = Address.fromString(nodeSettings.address);
			atomixBuilder.withMemberId(instanceSettings.id).withHost(address.host()).withPort(address.port());

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

			nodeId = nodeSettings.id;
			resultingEventTransformerSet = unmodifiableSet(resultingEventTransformerSet);
		}

		var bootstrapDiscoveryProviderNodes = new ArrayList<Node>();
		List<String> systemPartitionGroupMembers = new ArrayList<>();
		for (NodeSettings node : clusterSettings.nodes) {
			var address = Address.fromString(node.address);
			bootstrapDiscoveryProviderNodes.add(Node
					.builder()
					.withId(node.id)
					.withHost(address.host())
					.withPort(address.port())
					.build());
			systemPartitionGroupMembers.add(node.id);
		}

		var bootstrapDiscoveryProviderBuilder = BootstrapDiscoveryProvider.builder();
		bootstrapDiscoveryProviderBuilder.withNodes(bootstrapDiscoveryProviderNodes).build();

		atomixBuilder.withMembershipProvider(bootstrapDiscoveryProviderBuilder.build());

		atomixBuilder.withManagementGroup(RaftPartitionGroup
				.builder("system")
				.withNumPartitions(1)
				.withMembers(systemPartitionGroupMembers)
				.withDataDirectory(Paths.get(".data-" + instanceSettings.id).toFile())
				.build());

		atomixBuilder.withShutdownHook(false);
		atomixBuilder.withTypeRegistrationRequired();

		if (instanceSettings.client) {
			atomixBuilder.addProfile(Profile.client());
		} else {
			var prof = Profile.consensus(systemPartitionGroupMembers);
			var profCfg = (ConsensusProfileConfig) prof.config();
			//profCfg.setDataGroup("raft");
			profCfg.setDataPath(".data-" + instanceSettings.id);
			profCfg.setPartitions(3);
			atomixBuilder.addProfile(prof);
			//atomixBuilder.addProfile(Profile.dataGrid(32));
		}

		var atomix = atomixBuilder.build();

		TdSerializer.register(atomix.getSerializationService());

		atomix.start().join();

		var kafkaParameters = new KafkaParameters(clusterSettings, instanceSettings.id);

		var api = new AtomixReactiveApi(nodeId, atomix, kafkaParameters, diskSessions, resultingEventTransformerSet);

		LOG.info("Starting ReactiveApi...");

		api.start().block();

		LOG.info("Started ReactiveApi");

		return api;
	}

	public static void main(String[] args) throws IOException {
		var validArgs = parseArguments(args);
		var atomixBuilder = Atomix.builder().withShutdownHookEnabled();
		start(validArgs, atomixBuilder);
	}
}
