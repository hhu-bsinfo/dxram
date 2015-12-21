package de.uniduesseldorf.dxram.core.net;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.Tools;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class NetworkConfigurationValues {
	// IP address
	public static final ConfigurationEntry<String> NETWORK_IP = new ConfigurationEntry<String>("network.ip", String.class, Tools.getLocalIP());
	// Port
	public static final ConfigurationEntry<Integer> NETWORK_PORT = new ConfigurationEntry<Integer>("network.port", Integer.class, Tools.getFreePort(22222));
	// Max connection count at the same time
	public static final ConfigurationEntry<Integer> NETWORK_CONNECTIONS = new ConfigurationEntry<Integer>("network.connections", Integer.class, 100);
	// Size of the incoming message buffer (default 32 KB)
	public static final ConfigurationEntry<Integer> NETWORK_BUFFER_SIZE = new ConfigurationEntry<Integer>("network.buffer_size", Integer.class, 32 * 1024);
	//
	public static final ConfigurationEntry<Integer> NETWORK_MESSAGE_HANDLER_THREAD_COUNT = new ConfigurationEntry<Integer>(
			"network.message_handler_thread_count", Integer.class, 10);
	//
	public static final ConfigurationEntry<Integer> NETWORK_TASK_HANDLER_THREAD_COUNT = new ConfigurationEntry<Integer>(
			"network.task_handler_thread_count", Integer.class, 10);
	//
	public static final ConfigurationEntry<Integer> NETWORK_MAX_CACHE_SIZE = new ConfigurationEntry<Integer>("network.max_cache_size", Integer.class,
			128 * 1024 * 1024);
	//
	public static final ConfigurationEntry<Boolean> NETWORK_STATISTICS_THROUGHPUT = new ConfigurationEntry<Boolean>("network.statistics.throughput", Boolean.class,
			true);
	//
	public static final ConfigurationEntry<Boolean> NETWORK_STATISTICS_REQUESTS = new ConfigurationEntry<Boolean>("network..statistics.requests", Boolean.class,
			true);
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
		CONFIGURATION_ENTRIES.add(NETWORK_IP);
		CONFIGURATION_ENTRIES.add(NETWORK_PORT);
		CONFIGURATION_ENTRIES.add(NETWORK_CONNECTIONS);
		CONFIGURATION_ENTRIES.add(NETWORK_BUFFER_SIZE);
		CONFIGURATION_ENTRIES.add(NETWORK_MESSAGE_HANDLER_THREAD_COUNT);
		CONFIGURATION_ENTRIES.add(NETWORK_TASK_HANDLER_THREAD_COUNT);
		CONFIGURATION_ENTRIES.add(NETWORK_MAX_CACHE_SIZE);
		CONFIGURATION_ENTRIES.add(NETWORK_STATISTICS_THROUGHPUT);
		CONFIGURATION_ENTRIES.add(NETWORK_STATISTICS_REQUESTS);
	}
}
