
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Exception for failed component creations
 * @author Florian Klein
 *         23.03.2012
 */
public class ComponentCreationException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = 1188013683419380675L;

	// Constructors
	/**
	 * Creates an instance of ComponentCreationException
	 * @param p_message
	 *            the message
	 */
	public ComponentCreationException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of ComponentCreationException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public ComponentCreationException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
