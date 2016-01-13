
package de.hhu.bsinfo.dxram.engine;

/**
 * Exception for failed DXRAM accesses
 * @author Florian Klein
 *         09.03.2012
 */
public class DXRAMException extends Exception {

	// Constants
	private static final long serialVersionUID = 8402205300600257791L;

	// Constructors
	/**
	 * Creates an instance of DXRAMException
	 * @param p_message
	 *            the message
	 */
	public DXRAMException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of DXRAMException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public DXRAMException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
