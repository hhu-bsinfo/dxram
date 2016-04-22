
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the master salve service
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class MasterSlaveConfigurationValues {
	/**
	 * Configuration values for the service.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
	 */
	public static class Service {
		public static final Pair<String, String> ROLE = new Pair<String, String>("Role", "None");
		public static final Pair<String, Short> COMPUTE_GROUP_ID = new Pair<String, Short>("ComputeGroupId", (short) 0);
		public static final Pair<String, Integer> PING_INTERVAL_MS =
				new Pair<String, Integer>("PingIntervalMs", 1000);

	}
}
