
package de.hhu.bsinfo.dxram.recovery.messages;

/**
 * Encapsulates messages for the RecoveryService
 * @author Kevin Beinekechun
 *         03.06.2013
 */
public final class RecoveryMessages {

	// Constants
	public static final byte TYPE = 40;
	public static final byte SUBTYPE_RECOVER_MESSAGE = 1;
	public static final byte SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST = 2;
	public static final byte SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE = 3;

	/**
	 * Static class
	 */
	private RecoveryMessages() {};
}
