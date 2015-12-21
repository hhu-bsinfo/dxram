package de.uniduesseldorf.dxram.core.engine;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class DXRAMConfigurationValues {
	// DXRAM role (Superpeer, Peer or Monitor, may be overwritten by nodes configuration)
	public static final ConfigurationEntry<String> DXRAM_ROLE = new ConfigurationEntry<String>("dxram.role", String.class, "Peer");

	// Path in ZooKeeper
	public static final ConfigurationEntry<String> ZOOKEEPER_PATH = new ConfigurationEntry<String>("zookeeper.path", String.class, "/dxram");
	// Connection String for ZooKeeper
	public static final ConfigurationEntry<String> ZOOKEEPER_CONNECTION_STRING = new ConfigurationEntry<String>("zookeeper.connection_string",
			String.class, "127.0.0.1:2181");
	// Session Timeout for ZooKeeper
	public static final ConfigurationEntry<Integer> ZOOKEEPER_TIMEOUT = new ConfigurationEntry<Integer>("zookeeper.timeout", Integer.class, 10000);
	// Bitfield size
	public static final ConfigurationEntry<Integer> ZOOKEEPER_BITFIELD_SIZE = new ConfigurationEntry<Integer>("zookeeper.bitfield_size", Integer.class,
			256 * 1024);
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
		CONFIGURATION_ENTRIES.add(DXRAM_ROLE);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_PATH);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_CONNECTION_STRING);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_TIMEOUT);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_BITFIELD_SIZE);
	}
}
