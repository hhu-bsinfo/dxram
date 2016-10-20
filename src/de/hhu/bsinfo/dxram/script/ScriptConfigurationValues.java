
package de.hhu.bsinfo.dxram.script;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the script engine.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 14.10.16
 */
public class ScriptConfigurationValues {
	/**
	 * Configuration values for the script component.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 05.09.16
	 */
	public static class Component {
		public static final Pair<String, String> AUTOSTART_SCRIPT = new Pair<>("AutostartScript", "");
	}
}
