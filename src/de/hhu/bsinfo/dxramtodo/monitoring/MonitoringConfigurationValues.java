package de.hhu.bsinfo.dxramtodo.monitoring;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.utils.config.Configuration.ConfigurationEntry;

public class MonitoringConfigurationValues {
	// JNI console path
	public static final ConfigurationEntry<String> JNI_CONSOLE_DIRECTORY = new ConfigurationEntry<String>("console.jni_directory", String.class,
			"./jni/libJNIconsole.so");
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();
		
		CONFIGURATION_ENTRIES.add(JNI_CONSOLE_DIRECTORY);
	}
}
