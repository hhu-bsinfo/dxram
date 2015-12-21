package de.uniduesseldorf.dxram.core.lookup;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class LookupConfigurationValues {
	// Sleep interval
	public static final ConfigurationEntry<Integer> LOOKUP_SLEEP = new ConfigurationEntry<Integer>("lookup.sleep", Integer.class, 1);
	// Cache size
	public static final ConfigurationEntry<Integer> LOOKUP_CACHE_ENTRIES = new ConfigurationEntry<Integer>("lookup.cache_entries", Integer.class, 1000);
	// Cache ttl
	public static final ConfigurationEntry<Long> LOOKUP_CACHE_TTL = new ConfigurationEntry<Long>("lookup.cache_TTL", Long.class, (long) 1000);

	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();

		CONFIGURATION_ENTRIES.add(LOOKUP_SLEEP);
		CONFIGURATION_ENTRIES.add(LOOKUP_CACHE_ENTRIES);
		CONFIGURATION_ENTRIES.add(LOOKUP_CACHE_TTL);
	}
}
