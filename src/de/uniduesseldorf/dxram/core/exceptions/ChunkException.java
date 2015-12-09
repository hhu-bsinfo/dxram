
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Exception for failed data accesses
 * @author Florian Klein
 *         09.03.2012
 */
public class ChunkException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = -1678993183203711950L;

	// Constructors
	/**
	 * Creates an instance of DataException
	 * @param p_message
	 *            the message
	 */
	public ChunkException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of DataException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public ChunkException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
