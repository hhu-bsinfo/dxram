package de.uniduesseldorf.dxram.core.nameservice;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class NameserviceConfigurationValues {
	// Nameservice type
	public static final ConfigurationEntry<String> NAMESERVICE_TYPE = new ConfigurationEntry<String>("nameservice.type", String.class, "NAME");
	// Nameservice key length
	public static final ConfigurationEntry<Integer> NAMESERVICE_KEY_LENGTH = new ConfigurationEntry<Integer>("nameservice.key_length", Integer.class, 32);
	// Nameservice cache size
	
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();

		CONFIGURATION_ENTRIES.add(NAMESERVICE_CACHE_ENTRIES);
		CONFIGURATION_ENTRIES.add(NAMESERVICE_TYPE);
		CONFIGURATION_ENTRIES.add(NAMESERVICE_KEY_LENGTH);
	}
}
