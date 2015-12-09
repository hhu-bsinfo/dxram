
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Exception for failed memory accesses
 * @author Florian Klein
 *         21.03.2012
 */
public class MemoryException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = -4073927497992133209L;

	// Constructors
	/**
	 * Creates an instance of MemoryException
	 * @param p_message
	 *            the message
	 */
	public MemoryException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of MemoryException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public MemoryException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
