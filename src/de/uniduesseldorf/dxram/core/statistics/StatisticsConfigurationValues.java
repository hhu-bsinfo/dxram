package de.uniduesseldorf.dxram.core.statistics;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class StatisticsConfigurationValues {
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

	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();

		CONFIGURATION_ENTRIES.add(STATISTIC_PRINT);
		CONFIGURATION_ENTRIES.add(STATISTIC_CHUNK);
		CONFIGURATION_ENTRIES.add(STATISTIC_MEMORY);
		CONFIGURATION_ENTRIES.add(STATISTIC_REQUEST);
		CONFIGURATION_ENTRIES.add(STATISTIC_THROUGHPUT);
	}
}
