
package de.hhu.bsinfo.dxram.boot;

/**
 * Runtime exception thrown by the BootComponent.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class BootRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 7264524354993522197L;

	// Constructors
	/**
	 * Creates an instance of BootRuntimeException
	 * @param p_message
	 *            the message
	 */
	public BootRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of BootRuntimeException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public BootRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
