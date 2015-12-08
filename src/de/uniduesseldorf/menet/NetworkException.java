
package de.uniduesseldorf.menet;

/**
 * Exception for failed network accesses
 * @author Florian Klein
 *         09.03.2012
 */
public class NetworkException extends Exception {

	// Constants
	private static final long serialVersionUID = 4732144734226695683L;

	// Constructors
	/**
	 * Creates an instance of NetworkException
	 * @param p_message
	 *            the message
	 */
	public NetworkException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of NetworkException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public NetworkException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
