
package de.hhu.bsinfo.dxram.engine;

/**
 * Exception for failed DXRAM accesses
 * @author Florian Klein
 *         09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 25.01.16
 */
public class DXRAMRuntimeException extends RuntimeException {

	// Constants
	private static final long serialVersionUID = 7859427287438589879L;

	// Constructors
	/**
	 * Creates an instance of DXRAMRuntimeException
	 * @param p_message
	 *            the message
	 */
	public DXRAMRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of DXRAMRuntimeException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public DXRAMRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
