package de.uniduesseldorf.soh.exception;

public class OutOfBlockSizeBounds extends MemoryException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6722708030348749704L;

	public OutOfBlockSizeBounds(String p_message) {
		super(p_message);
	}
	
	public OutOfBlockSizeBounds(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
