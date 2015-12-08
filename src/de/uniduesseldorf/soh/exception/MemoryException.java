package de.uniduesseldorf.soh.exception;

public class MemoryException extends Exception {

	private static final long serialVersionUID = 1603108931665912402L;

	public MemoryException(final String p_message) {
		super(p_message);
	}

	public MemoryException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
