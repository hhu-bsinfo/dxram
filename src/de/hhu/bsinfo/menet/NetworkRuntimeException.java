
package de.hhu.bsinfo.menet;

/**
 * A netork runtime exception
 * @author Stefan Nothaas <stefan.nothass@hhu.de> 01.02.16
 */
class NetworkRuntimeException extends RuntimeException {
	private static final long serialVersionUID = -1801173917259116729L;

	/**
	 * Creates an instance of NetworkRuntimeException
	 * @param p_message
	 *            the message
	 */
	protected NetworkRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of NetworkRuntimeException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	protected NetworkRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
