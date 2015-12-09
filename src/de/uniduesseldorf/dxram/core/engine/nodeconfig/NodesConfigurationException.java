package de.uniduesseldorf.dxram.core.engine.nodeconfig;

/**
 * Exception that can be thrown by anything in this package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 *
 */
public class NodesConfigurationException extends Exception {

	private static final long serialVersionUID = -6500189956792522194L;

	/**
	 * Constructor
	 * @param p_message Exception message.
	 */
	public NodesConfigurationException(final String p_message) {
		super(p_message);
	}

	/**
	 * Constructor
	 * @param p_message Exception message.
	 * @param p_cause Other exception causing this exception.
	 */
	public NodesConfigurationException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
