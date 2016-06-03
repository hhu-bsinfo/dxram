
package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the zookeeper boot component implementation.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class ZookeeperBootConfigurationValues {
	/**
	 * Configuration values for the component.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
	 */
	public static final class Component {
		public static final Pair<String, String> PATH = new Pair<String, String>("Path", "/dxram");
		public static final Pair<String, String> CONNECTION_STRING = new Pair<String, String>("ConnectionString", "127.0.0.1:2181");
		public static final Pair<String, Integer> TIMEOUT = new Pair<String, Integer>("Timeout", 10000);
		public static final Pair<String, Integer> BITFIELD_SIZE = new Pair<String, Integer>("BitfieldSize", 256 * 1024);

		/**
		 * Utils class.
		 */
		private Component() {}
	}
}
