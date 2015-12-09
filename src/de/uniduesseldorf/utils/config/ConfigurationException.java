package de.uniduesseldorf.utils.config;

public class ConfigurationException extends Exception {

	private static final long serialVersionUID = -21699015441877820L;

	public ConfigurationException(final String p_message) {
		super(p_message);
	}

	public ConfigurationException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
