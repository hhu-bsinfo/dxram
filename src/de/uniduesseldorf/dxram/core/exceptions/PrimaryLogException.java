
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Exception for failed logging accesses
 * @author Florian Klein
 *         21.03.2012
 */
public class PrimaryLogException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = -1606349411876483752L;

	// Constructors
	/**
	 * Creates an instance of LogException
	 * @param p_message
	 *            the message
	 */
	public PrimaryLogException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of LogException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public PrimaryLogException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
