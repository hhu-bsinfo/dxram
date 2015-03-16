
package de.uniduesseldorf.dxram;

import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * Logger for DXRAM
 * @author Florian Klein
 *         09.03.2012
 */
public final class DXRAMLogger {

	// Constants
	private static final Logger LOGGER = Logger.getLogger("de.uniduesseldorf.dxram");

	public static final boolean TRACE = true;
	public static final boolean DEBUG = true;
	public static final boolean INFO = true;
	public static final boolean WARN = true;
	public static final boolean ERROR = true;
	public static final boolean FATAL = true;

	// Constructors
	/**
	 * Creates an instance of DXRAMLogger
	 */
	private DXRAMLogger() {}

	// Methods
	/**
	 * Traces a method enter in the log
	 * @param p_parameters
	 *            the method parameters
	 */
	public static void traceEnterMethod(final Object... p_parameters) {
		if (TRACE) {
			if (p_parameters != null && p_parameters.length > 0) {
				LOGGER.trace("Entering " + Thread.currentThread().getStackTrace()[2].toString() + " with: "
						+ Arrays.toString(p_parameters));
			} else {
				LOGGER.trace("Entering " + Thread.currentThread().getStackTrace()[2].toString());
			}
		}
	}

	/**
	 * Traces a method exit in the log
	 * @param p_return
	 *            the methord return value
	 */
	public static void traceExitMethod(final Object p_return) {
		if (TRACE) {
			if (p_return != null) {
				LOGGER.trace("Exiting " + Thread.currentThread().getStackTrace()[2].toString() + " with: " + p_return);
			} else {
				LOGGER.trace("Exiting " + Thread.currentThread().getStackTrace()[2].toString());
			}
		}
	}

	/**
	 * Logs a trace message
	 * @param p_message
	 *            the trace messsage
	 */
	public static void trace(final String p_message) {
		if (TRACE) {
			LOGGER.trace(p_message);
		}
	}

	/**
	 * Logs a debug message
	 * @param p_message
	 *            the debug messsage
	 */
	public static void debug(final String p_message) {
		if (DEBUG) {
			LOGGER.debug(p_message);
		}
	}

	/**
	 * Logs a info
	 * @param p_message
	 *            the info messsage
	 */
	public static void info(final String p_message) {
		if (INFO) {
			LOGGER.info(p_message);
		}
	}

	/**
	 * Logs a warning
	 * @param p_message
	 *            the warning messsage
	 */
	public static void warn(final String p_message) {
		if (WARN) {
			LOGGER.warn(p_message);
		}
	}

	/**
	 * Logs an error
	 * @param p_message
	 *            the error message
	 * @param p_exception
	 *            the execption
	 */
	public static void error(final String p_message, final Exception p_exception) {
		if (ERROR) {
			LOGGER.error(p_message, p_exception);
		}
	}

	/**
	 * Logs a fatal system state
	 * @param p_message
	 *            the system state message
	 * @param p_exception
	 *            the execption
	 */
	public static void fatal(final String p_message, final Exception p_exception) {
		if (FATAL) {
			LOGGER.fatal(p_message, p_exception);
		}
	}

}
