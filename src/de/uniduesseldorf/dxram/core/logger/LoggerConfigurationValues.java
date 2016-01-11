package de.uniduesseldorf.dxram.core.logger;

import de.uniduesseldorf.dxram.core.util.logger.LoggerLog4j;

import de.uniduesseldorf.utils.Pair;

public class LoggerConfigurationValues {
	public static class Component {
		public static final Pair<String, String> LOG_LEVEL = new Pair<String, String>("DefaultLogLevel", "trace");
		public static final Pair<String, String> LOGGER_IMPL = new Pair<String, String>("Logger", LoggerLog4j.class.getName());
	}
}
