
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the terminal.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 05.09.16
 */
public class TerminalConfigurationValues {
	/**
	 * Configuration values for the terminal component.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 05.09.16
	 */
	public static class Component {
		public static final Pair<String, Boolean> ASK_ON_QUIT = new Pair<>("AskOnQuit", true);
	}

	/**
	 * Configuration values for the terminal service.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 05.09.16
	 */
	public static class Service {
		public static final Pair<String, String> AUTOSTART_SCRIPT = new Pair<>("AutostartScript", "");
	}
}
