package de.uniduesseldorf.dxram.core.mem;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class MemoryManagerConfigurationValues {
	// Total size of local (chunk) memory
	public static final ConfigurationEntry<Long> MEM_SIZE = new ConfigurationEntry<Long>("mem.size", Long.class, 1073741824L);
	// Size for each segment for the memory
	public static final ConfigurationEntry<Long> MEM_SEGMENT_SIZE = new ConfigurationEntry<Long>("mem.segment_size", Long.class, 1073741824L);

	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
		CONFIGURATION_ENTRIES.add(MEM_SIZE);
		CONFIGURATION_ENTRIES.add(MEM_SEGMENT_SIZE);
	}
}
