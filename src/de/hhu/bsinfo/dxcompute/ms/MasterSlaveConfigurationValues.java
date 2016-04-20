
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.utils.Pair;

public class MasterSlaveConfigurationValues {
	public static class Service {
		public static final Pair<String, String> ROLE = new Pair<String, String>("Role", "None");
		public static final Pair<String, Integer> COMPUTE_GROUP_ID = new Pair<String, Integer>("ComputeGroupId", 0);
		public static final Pair<String, Integer> PING_INTERVAL_MS =
				new Pair<String, Integer>("PingIntervalMs", 1000);

	}
}
