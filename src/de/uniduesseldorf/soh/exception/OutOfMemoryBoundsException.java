package de.uniduesseldorf.soh.exception;

public class OutOfMemoryBoundsException extends MemoryException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3427732100224428839L;

	public OutOfMemoryBoundsException(String p_message) {
		super(p_message);
	}
	
	public OutOfMemoryBoundsException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}

}
