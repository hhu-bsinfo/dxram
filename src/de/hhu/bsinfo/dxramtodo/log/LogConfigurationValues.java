package de.hhu.bsinfo.dxram.log;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.utils.config.Configuration.ConfigurationEntry;

public class LogConfigurationValues {
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
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
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
	}
}
