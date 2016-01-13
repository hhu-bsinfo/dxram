package de.hhu.bsinfo.menet;

public class NetworkRuntimeException extends RuntimeException
{
	private static final long serialVersionUID = -1801173917259116729L;

	public NetworkRuntimeException(final String p_message) {
		super(p_message);
	}

	public NetworkRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
