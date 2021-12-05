package it.tdlight.reactiveapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.profile.Profile;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Entrypoint {

	private static final Logger LOG = LoggerFactory.getLogger(Entrypoint.class);

	public static record ValidEntrypointArgs(String clusterPath, String instancePath, String diskSessionsPath) {}

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
			diskSessions = new DiskSessionsManager(mapper, diskSessionsConfigPath);
		}

		atomixBuilder.withClusterId(clusterSettings.id);

		if (instanceSettings.client) {
			atomixBuilder.withMemberId(instanceSettings.id).withAddress(instanceSettings.clientAddress);
		} else {
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

			atomixBuilder.withMemberId(instanceSettings.id).withAddress(nodeSettings.address);
		}

		var bootstrapDiscoveryProviderNodes = new ArrayList<Node>();
		List<String> systemPartitionGroupMembers = new ArrayList<>();
		for (NodeSettings node : clusterSettings.nodes) {
			bootstrapDiscoveryProviderNodes.add(Node.builder().withId(node.id).withAddress(node.address).build());
			systemPartitionGroupMembers.add(node.id);
		}

		var bootstrapDiscoveryProviderBuilder = BootstrapDiscoveryProvider.builder();
		bootstrapDiscoveryProviderBuilder.withNodes(bootstrapDiscoveryProviderNodes).build();

		atomixBuilder.withMembershipProvider(bootstrapDiscoveryProviderBuilder.build());

		atomixBuilder.withManagementGroup(RaftPartitionGroup
				.builder("system")
				.withNumPartitions(1)
				.withMembers(systemPartitionGroupMembers)
				.build());

		atomixBuilder.withPartitionGroups(PrimaryBackupPartitionGroup.builder("data").withNumPartitions(32).build());

		atomixBuilder.withShutdownHook(false);
		atomixBuilder.withTypeRegistrationRequired();

		if (instanceSettings.client) {
			atomixBuilder.addProfile(Profile.consensus(systemPartitionGroupMembers));
			atomixBuilder.addProfile(Profile.dataGrid(32));
		} else {
			atomixBuilder.addProfile(Profile.client());
		}

		atomixBuilder.withCompatibleSerialization(false);

		var atomix = atomixBuilder.build();

		TdSerializer.register(atomix.getSerializationService());

		atomix.start().join();

		var api = new ReactiveApi(atomix, diskSessions);

		LOG.info("Starting ReactiveApi...");

		api.start();

		return api;
	}

	public static void main(String[] args) throws IOException {
		var validArgs = parseArguments(args);
		var atomixBuilder = Atomix.builder().withShutdownHookEnabled();
		start(validArgs, atomixBuilder);
	}
}
