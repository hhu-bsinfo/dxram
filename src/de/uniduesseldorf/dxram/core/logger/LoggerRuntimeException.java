package de.uniduesseldorf.dxram.core.logger;

public class LoggerRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 5401438020051542241L;

	/**
	 * Creates an instance of LoggerRuntimeException
	 * @param p_message
	 *            the message
	 */
	public LoggerRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of LoggerRuntimeException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public LoggerRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
