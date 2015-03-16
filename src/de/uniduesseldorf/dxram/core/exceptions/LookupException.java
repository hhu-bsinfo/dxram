
package de.uniduesseldorf.dxram.core.exceptions;

/**
 * Exception for failed meta data accesses
 * @author Florian Klein
 *         09.03.2012
 */
public class LookupException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = 5917319024322071829L;

	// Constructors
	/**
	 * Creates an instance of LookupException
	 * @param p_message
	 *            the message
	 */
	public LookupException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of LookupException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public LookupException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
