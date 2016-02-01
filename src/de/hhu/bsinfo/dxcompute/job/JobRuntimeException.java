package de.hhu.bsinfo.dxcompute.job;

/**
 * Runtime exception for non recoverable failure in job package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 01.02.16
 */
public class JobRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 805380076097533428L;

	/**
	 * Creates an instance of JobRuntimeException
	 * @param p_message
	 *            the message
	 */
	public JobRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Creates an instance of JobRuntimeException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public JobRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
