
package de.hhu.bsinfo.soh;

/**
 * Runtime memory exception for the small object heap.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public class MemoryRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1547073091176402925L;

	/**
	 * Constructor
	 * @param p_message
	 *            Exception message.
	 */
	public MemoryRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Constructor
	 * @param p_message
	 *            Exception message
	 * @param p_cause
	 *            Other exception causing this
	 */
	public MemoryRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
