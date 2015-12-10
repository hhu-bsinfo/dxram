package de.uniduesseldorf.dxram.core.engine.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.uniduesseldorf.utils.Tools;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

/**
 * Stores all available ConfigurationEntries
 * @author Florian Klein
 *         03.09.2013
 */
public final class DXRAMConfigurationConstants {

	// Constants

	// DXRAM role (Superpeer, Peer or Monitor, may be overwritten by nodes configuration)
	public static final ConfigurationEntry<String> DXRAM_ROLE = new ConfigurationEntry<String>("dxram.role", String.class, "Peer");

//	// Class for the ChunkInterface
//	public static final ConfigurationEntry<String> INTERFACE_CHUNK = new ConfigurationEntry<String>("interface.chunk", String.class,
//			"de.uniduesseldorf.dxram.core.chunk.ChunkHandler");
//	// Class for the LookupInterface
//	public static final ConfigurationEntry<String> INTERFACE_LOOKUP = new ConfigurationEntry<String>("interface.lookup", String.class,
//			"de.uniduesseldorf.dxram.core.lookup.CachedTreeLookup");
//	// Class for the ZooKeeperInterface
//	public static final ConfigurationEntry<String> INTERFACE_ZOOKEEPER = new ConfigurationEntry<String>("interface.zookeeper", String.class,
//			"de.uniduesseldorf.dxram.core.zookeeper.ZooKeeperHandler");
//	// Class for the NetworkInterface
//	public static final ConfigurationEntry<String> INTERFACE_NETWORK = new ConfigurationEntry<String>("interface.network", String.class,
//			"de.uniduesseldorf.dxram.core.net.NetworkHandler");
//	// Class for the MemoryInterface
//	public static final ConfigurationEntry<String> INTERFACE_RAM = new ConfigurationEntry<String>("interface.ram", String.class,
//			"de.uniduesseldorf.dxram.core.chunk.storage.UnsafeRAMHandler");
//	// Class for the LogInterface
//	public static final ConfigurationEntry<String> INTERFACE_LOG = new ConfigurationEntry<String>("interface.log", String.class,
//			"de.uniduesseldorf.dxram.core.log.LogHandler");
//	// Class for the RecoveryInterface
//	public static final ConfigurationEntry<String> INTERFACE_RECOVERY = new ConfigurationEntry<String>("interface.recovery", String.class,
//			"de.uniduesseldorf.dxram.core.recovery.RecoveryHandler");
//	// Class for the LockInterface
//	public static final ConfigurationEntry<String> INTERFACE_LOCK = new ConfigurationEntry<String>("interface.lock", String.class,
//			"de.uniduesseldorf.dxram.core.lock.DefaultLockHandler");

	// Local IP address
	public static final ConfigurationEntry<String> NETWORK_IP = new ConfigurationEntry<String>("network.ip", String.class, Tools.getLocalIP());
	// Global Port for DXRAM
	public static final ConfigurationEntry<Integer> NETWORK_PORT = new ConfigurationEntry<Integer>("network.port", Integer.class, Tools.getFreePort(22222));
	// Max connection count at the same time
	public static final ConfigurationEntry<Integer> NETWORK_CONNECTIONS = new ConfigurationEntry<Integer>("network.connections", Integer.class, 100);
	// Size of the incoming message buffer (default 32 KB)
	public static final ConfigurationEntry<Integer> NETWORK_BUFFER_SIZE = new ConfigurationEntry<Integer>("network.buffer_size", Integer.class, 32 * 1024);
	// Class for creating new network connections (default creator uses Java NIO)
	public static final ConfigurationEntry<String> NETWORK_CREATOR = new ConfigurationEntry<String>("network.creator", String.class,
			"de.uniduesseldorf.dxram.core.net.NIOConnectionCreator");
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
	
	// Size of the RAM
	public static final ConfigurationEntry<Long> RAM_SIZE = new ConfigurationEntry<Long>("ram.size", Long.class, 1073741824L);
	// Size of the RAM
	public static final ConfigurationEntry<Long> RAM_SEGMENT_SIZE = new ConfigurationEntry<Long>("ram.segment_size", Long.class, 1073741824L);
	// Class for the Memory-Management of the RAM
	public static final ConfigurationEntry<String> RAM_MANAGEMENT = new ConfigurationEntry<String>("ram.management", String.class,
			"de.uniduesseldorf.dxram.core.chunk.storage.SimpleListStorageManagement");

	// Max size of a Chunk (default 16 MB)
	public static final ConfigurationEntry<Integer> CHUNK_MAX_SIZE = new ConfigurationEntry<Integer>("chunk.max_size", Integer.class, 16 * 1024 * 1024);

	// Backup activation flag
	public static final ConfigurationEntry<Boolean> LOG_ACTIVE = new ConfigurationEntry<Boolean>("log.active", Boolean.class, false);
	// Checksum usage flag
	public static final ConfigurationEntry<Boolean> LOG_CHECKSUM = new ConfigurationEntry<Boolean>("log.checksum", Boolean.class, false);
	// Replication factor
	public static final ConfigurationEntry<Integer> REPLICATION_FACTOR = new ConfigurationEntry<Integer>("log.replication_factor", Integer.class, 3);
	// Size of the primary log file (default 8 GB)
	public static final ConfigurationEntry<Long> PRIMARY_LOG_SIZE = new ConfigurationEntry<Long>("log.primary_size", Long.class, 2 * 1024 * 1024 * 1024L);
	// Size of the secondary log file (default 512 MB)
	public static final ConfigurationEntry<Long> SECONDARY_LOG_SIZE = new ConfigurationEntry<Long>("log.secondary_size", Long.class, 512 * 1024 * 1024L);
	// Size of the write buffer (default 256 MB)
	public static final ConfigurationEntry<Integer> WRITE_BUFFER_SIZE =
			new ConfigurationEntry<Integer>("log.buffer_size", Integer.class, 256 * 1024 * 1024);
	// Size of the segments (default 8 MB)
	public static final ConfigurationEntry<Integer> LOG_SEGMENT_SIZE = new ConfigurationEntry<Integer>("log.segment_size", Integer.class, 8 * 1024 * 1024);
	// Size of the segments (default 8 MB)
	public static final ConfigurationEntry<Integer> FLASHPAGE_SIZE = new ConfigurationEntry<Integer>("log.flashpage_size", Integer.class, 4096);
	// Size of the segments (default 8 MB)
	public static final ConfigurationEntry<Integer> REORG_UTILIZATION_THRESHOLD = new ConfigurationEntry<Integer>("log.reorg_util_threshold",
			Integer.class, 70);
	// Directory for log files
	public static final ConfigurationEntry<String> LOG_DIRECTORY = new ConfigurationEntry<String>("log.directory", String.class, "./log/");
	// Write buffer synchronization method
	public static final ConfigurationEntry<Boolean> LOG_PARALLEL_BUFFERING =
			new ConfigurationEntry<Boolean>("log.parallel_buffering", Boolean.class, false);

	// Sleep interval
	public static final ConfigurationEntry<Integer> LOOKUP_SLEEP = new ConfigurationEntry<Integer>("lookup.sleep", Integer.class, 1);
	// Cache size
	public static final ConfigurationEntry<Integer> LOOKUP_CACHE_ENTRIES = new ConfigurationEntry<Integer>("lookup.cache_entries", Integer.class, 1000);
	// Cache ttl
	public static final ConfigurationEntry<Long> LOOKUP_CACHE_TTL = new ConfigurationEntry<Long>("lookup.cache_TTL", Long.class, (long) 1000);

	// Nameservice type
	public static final ConfigurationEntry<String> NAMESERVICE_TYPE = new ConfigurationEntry<String>("nameservice.type", String.class, "NAME");
	// Nameservice key length
	public static final ConfigurationEntry<Integer> NAMESERVICE_KEY_LENGTH = new ConfigurationEntry<Integer>("nameservice.key_length", Integer.class, 32);
	// Nameservice cache size
	public static final ConfigurationEntry<Integer> NAMESERVICE_CACHE_ENTRIES = new ConfigurationEntry<Integer>("nameservice.cache_entries", Integer.class,
			1000000);

	// JNI lock path
	public static final ConfigurationEntry<String> JNI_LOCK_DIRECTORY = new ConfigurationEntry<String>("lock.jni_directory", String.class,
			"./jni/libJNILock.so");

	// JNI console path
	public static final ConfigurationEntry<String> JNI_CONSOLE_DIRECTORY = new ConfigurationEntry<String>("console.jni_directory", String.class,
			"./jni/libJNIconsole.so");

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

	// Periodically print statistics?
	public static final ConfigurationEntry<Integer> STATISTIC_PRINT = new ConfigurationEntry<Integer>("statistic.print", Integer.class, 0);
	// Chunk-Statistic
	public static final ConfigurationEntry<Boolean> STATISTIC_CHUNK = new ConfigurationEntry<Boolean>("statistic.chunk", Boolean.class, true);
	// Memory-Statistic
	public static final ConfigurationEntry<Boolean> STATISTIC_MEMORY = new ConfigurationEntry<Boolean>("statistic.memory", Boolean.class, true);
	// Request-Statistic
	public static final ConfigurationEntry<Boolean> STATISTIC_REQUEST = new ConfigurationEntry<Boolean>("statistic.request", Boolean.class, true);
	// Throughput-Statistic
	public static final ConfigurationEntry<Boolean> STATISTIC_THROUGHPUT = new ConfigurationEntry<Boolean>("statistic.throughput", Boolean.class, true);

	private static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
		CONFIGURATION_ENTRIES.add(DXRAM_ROLE);
		CONFIGURATION_ENTRIES.add(CHUNK_MAX_SIZE);
//		CONFIGURATION_ENTRIES.add(INTERFACE_CHUNK);
//		CONFIGURATION_ENTRIES.add(INTERFACE_LOOKUP);
//		CONFIGURATION_ENTRIES.add(INTERFACE_ZOOKEEPER);
//		CONFIGURATION_ENTRIES.add(INTERFACE_NETWORK);
//		CONFIGURATION_ENTRIES.add(INTERFACE_RAM);
//		CONFIGURATION_ENTRIES.add(INTERFACE_LOG);
//		CONFIGURATION_ENTRIES.add(INTERFACE_RECOVERY);
//		CONFIGURATION_ENTRIES.add(INTERFACE_LOCK);
		CONFIGURATION_ENTRIES.add(NETWORK_IP);
		CONFIGURATION_ENTRIES.add(NETWORK_PORT);
		CONFIGURATION_ENTRIES.add(NETWORK_CONNECTIONS);
		CONFIGURATION_ENTRIES.add(NETWORK_BUFFER_SIZE);
		CONFIGURATION_ENTRIES.add(NETWORK_CREATOR);
		CONFIGURATION_ENTRIES.add(NETWORK_MESSAGE_HANDLER_THREAD_COUNT);
		CONFIGURATION_ENTRIES.add(NETWORK_TASK_HANDLER_THREAD_COUNT);
		CONFIGURATION_ENTRIES.add(NETWORK_MAX_CACHE_SIZE);
		CONFIGURATION_ENTRIES.add(RAM_SIZE);
		CONFIGURATION_ENTRIES.add(RAM_MANAGEMENT);
		CONFIGURATION_ENTRIES.add(LOG_ACTIVE);
		CONFIGURATION_ENTRIES.add(LOG_CHECKSUM);
		CONFIGURATION_ENTRIES.add(REPLICATION_FACTOR);
		CONFIGURATION_ENTRIES.add(PRIMARY_LOG_SIZE);
		CONFIGURATION_ENTRIES.add(SECONDARY_LOG_SIZE);
		CONFIGURATION_ENTRIES.add(WRITE_BUFFER_SIZE);
		CONFIGURATION_ENTRIES.add(LOG_SEGMENT_SIZE);
		CONFIGURATION_ENTRIES.add(FLASHPAGE_SIZE);
		CONFIGURATION_ENTRIES.add(REORG_UTILIZATION_THRESHOLD);
		CONFIGURATION_ENTRIES.add(LOG_DIRECTORY);
		CONFIGURATION_ENTRIES.add(LOG_PARALLEL_BUFFERING);
		CONFIGURATION_ENTRIES.add(LOOKUP_SLEEP);
		CONFIGURATION_ENTRIES.add(LOOKUP_CACHE_ENTRIES);
		CONFIGURATION_ENTRIES.add(LOOKUP_CACHE_TTL);
		CONFIGURATION_ENTRIES.add(NAMESERVICE_CACHE_ENTRIES);
		CONFIGURATION_ENTRIES.add(NAMESERVICE_TYPE);
		CONFIGURATION_ENTRIES.add(NAMESERVICE_KEY_LENGTH);
		CONFIGURATION_ENTRIES.add(JNI_LOCK_DIRECTORY);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_PATH);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_CONNECTION_STRING);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_TIMEOUT);
		CONFIGURATION_ENTRIES.add(ZOOKEEPER_BITFIELD_SIZE);
		CONFIGURATION_ENTRIES.add(STATISTIC_PRINT);
		CONFIGURATION_ENTRIES.add(STATISTIC_CHUNK);
		CONFIGURATION_ENTRIES.add(STATISTIC_MEMORY);
		CONFIGURATION_ENTRIES.add(STATISTIC_REQUEST);
		CONFIGURATION_ENTRIES.add(STATISTIC_THROUGHPUT);

		Collections.sort(CONFIGURATION_ENTRIES, new ConfigurationEntryComparator());
	}

	// Constructors
	/**
	 * Creates an instance of ConfigurationConstants
	 */
	private DXRAMConfigurationConstants() {}

	// Getters
	/**
	 * Gets a sorted List of all ConfigurationEntries
	 * @return a sorted List of all ConfigurationEntries
	 */
	public static Collection<ConfigurationEntry<?>> getConfigurationEntries() {
		return CONFIGURATION_ENTRIES;
	}

	// Methods
	/**
	 * Gets the corresponding ConfigurationEntry for the given key
	 * @param p_key
	 *            the key of the ConfigurationEntry
	 * @return the corresponding ConfigurationEntry
	 */
	public static ConfigurationEntry<?> getConfigurationEntry(final String p_key) {
		ConfigurationEntry<?> ret = null;

		for (ConfigurationEntry<?> entry : CONFIGURATION_ENTRIES) {
			if (entry.getKey().equals(p_key)) {
				ret = entry;

				break;
			}
		}

		return ret;
	}

	// Classes
	/**
	 * Comparator for the ConfigurationEntries
	 * @author Florian Klein
	 *         03.09.2013
	 */
	private static final class ConfigurationEntryComparator implements Comparator<ConfigurationEntry<?>> {

		// Constructors
		/**
		 * Creates an instance of ConfigurationEntryComparator
		 */
		private ConfigurationEntryComparator() {}

		// Methods
		@Override
		public int compare(final ConfigurationEntry<?> p_entry1, final ConfigurationEntry<?> p_entry2) {
			return p_entry1.getKey().compareTo(p_entry2.getKey());
		}

	}

}