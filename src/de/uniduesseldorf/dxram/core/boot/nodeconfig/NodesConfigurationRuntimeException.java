package de.uniduesseldorf.dxram.core.engine.nodeconfig;

/**
 * Exception that is thrown by anything in this package, but can not be handled.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 *
 */
public class NodesConfigurationRuntimeException extends RuntimeException {

	private static final long serialVersionUID = -3783504252255902613L;

	/**
	 * Constructor
	 * @param p_message Exception message.
	 */
	public NodesConfigurationRuntimeException(final String p_message) {
		super(p_message);
	}

	/**
	 * Constructor
	 * @param p_message Exception message.
	 * @param p_cause Other exception causing this one.
	 */
	public NodesConfigurationRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
