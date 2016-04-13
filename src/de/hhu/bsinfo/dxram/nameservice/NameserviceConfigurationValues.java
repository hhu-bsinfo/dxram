
package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the name service.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class NameserviceConfigurationValues {
	/**
	 * Configuration values for the name service component.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
	 */
	public static class Component {
		public static final Pair<String, String> TYPE = new Pair<String, String>("Type", "NAME");
	}
}
