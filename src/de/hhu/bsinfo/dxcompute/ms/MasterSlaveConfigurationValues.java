
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the master salve service
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
class MasterSlaveConfigurationValues {
	/**
	 * Configuration values for the service.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
	 */
	public static class Service {
		static final Pair<String, String> ROLE = new Pair<>("Role", "None");
		static final Pair<String, Short> COMPUTE_GROUP_ID = new Pair<>("ComputeGroupId", (short) 0);
		static final Pair<String, Integer> PING_INTERVAL_MS =
				new Pair<>("PingIntervalMs", 1000);

	}
}
