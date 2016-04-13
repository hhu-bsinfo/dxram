
package de.hhu.bsinfo.dxram.event;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the event package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class EventConfigurationValues {
	/**
	 * Configuration values for the component
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 */
	public static class Component {
		public static final Pair<String, Boolean> USE_EXECUTOR = new Pair<String, Boolean>("UseExecutor", true);
		public static final Pair<String, Integer> THREAD_COUNT = new Pair<String, Integer>("ThreadCount", 1);
	}
}
