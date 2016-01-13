package de.hhu.bsinfo.soh.exception;

public class MemoryRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1547073091176402925L;

	public MemoryRuntimeException(final String p_message) {
		super(p_message);
	}

	public MemoryRuntimeException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
	}
}
