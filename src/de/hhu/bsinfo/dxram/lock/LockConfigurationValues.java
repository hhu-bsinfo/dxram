package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for lock component and service.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LockConfigurationValues {
	public static class Service {
		public static final Pair<String, Integer> REMOTE_LOCK_SEND_INTERVAL_MS = new Pair<String, Integer>("RemoteLockSendIntervalMs", 10);
		public static final Pair<String, Integer> REMOTE_LOCK_TRY_TIMEOUT_MS = new Pair<String, Integer>("RemoteLockTryTimeoutMs", 100);
		public static final Pair<String, Boolean> STATISTICS = new Pair<String, Boolean>("Statistics", false);
	}
}
