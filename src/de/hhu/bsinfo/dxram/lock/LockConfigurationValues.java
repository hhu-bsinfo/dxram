package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.utils.Pair;

public class LockConfigurationValues {
	// TODO move this to engine configuration?
//	// JNI lock path
//	public static final ConfigurationEntry<String> JNI_LOCK_DIRECTORY = new ConfigurationEntry<String>("lock.jni_directory", String.class,
//			"./jni/libJNILock.so");
	
	public static class Service {
		public static final Pair<String, Integer> REMOTE_LOCK_SEND_INTERVAL_MS = new Pair<String, Integer>("RemoteLockSendIntervalMs", 10);
		public static final Pair<String, Integer> REMOTE_LOCK_TRY_TIMEOUT_MS = new Pair<String, Integer>("RemoteLockTryTimeoutMs", 100);
		public static final Pair<String, Boolean> STATISTICS = new Pair<String, Boolean>("Statistics", false);
	}
}
