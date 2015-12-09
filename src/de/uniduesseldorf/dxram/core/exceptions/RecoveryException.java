
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Exception for failed recovery accesses
 * @author Florian Klein
 *         21.03.2012
 */
public class RecoveryException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = 3444442654568040590L;

	// Constructors
	/**
	 * Creates an instance of RecoveryException
	 * @param p_message
	 *            the message
	 */
	public RecoveryException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of RecoveryException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public RecoveryException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
