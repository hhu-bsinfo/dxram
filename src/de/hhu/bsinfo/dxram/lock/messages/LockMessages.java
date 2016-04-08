
package de.hhu.bsinfo.dxram.lock.messages;

/**
 * Network message types for the lock package
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 5.1.16
 */
public final class LockMessages {
	public static final byte TYPE = 3;
	public static final byte SUBTYPE_LOCK_REQUEST = 1;
	public static final byte SUBTYPE_LOCK_RESPONSE = 2;
	public static final byte SUBTYPE_UNLOCK_MESSAGE = 3;

	/**
	 * Static class
	 */
	private LockMessages() {}
}
