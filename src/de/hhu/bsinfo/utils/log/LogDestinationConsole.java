
package de.hhu.bsinfo.utils.log;

/**
 * Implementation to log to the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LogDestinationConsole implements LogDestination {

	@Override
	public void log(final LogLevel p_level, final String p_message) {
		// add some color
		switch (p_level) {
			case TRACE:
				System.out.println("\033[0;37m" + p_message + "\033[0m");
				break;
			case DEBUG:
				System.out.println("\033[1;37m" + p_message + "\033[0m");
				break;
			case INFO:
				System.out.println("\033[1;34m" + p_message + "\033[0m");
				break;
			case WARN:
				System.out.println("\033[1;33m" + p_message + "\033[0m");
				break;
			case ERROR:
				System.out.println("\033[1;31m" + p_message + "\033[0m");
				break;
			default:
				break;
		}
	}
}
