package de.uniduesseldorf.dxram.core.lock;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

public class LockConfigurationValues {
	// JNI lock path
	public static final ConfigurationEntry<String> JNI_LOCK_DIRECTORY = new ConfigurationEntry<String>("lock.jni_directory", String.class,
			"./jni/libJNILock.so");
	
	public static final List<ConfigurationEntry<?>> CONFIGURATION_ENTRIES;
	static {
		CONFIGURATION_ENTRIES = new ArrayList<>();

		CONFIGURATION_ENTRIES.add(JNI_LOCK_DIRECTORY);
	}
}
