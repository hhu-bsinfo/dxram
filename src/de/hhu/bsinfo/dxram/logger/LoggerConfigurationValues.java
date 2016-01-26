package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the logger component.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LoggerConfigurationValues {
	public static class Component {
		public static final Pair<String, String> LOG_LEVEL = new Pair<String, String>("DefaultLogLevel", "trace");
	}
}
