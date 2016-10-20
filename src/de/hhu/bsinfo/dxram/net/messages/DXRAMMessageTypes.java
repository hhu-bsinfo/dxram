
package de.hhu.bsinfo.dxram.net.messages;

/**
 * Type and list of types for all dxram messages
 * @author Kevin Beineke <kevin.beineke@hhu.de> 19.10.16
 */
public class DXRAMMessageTypes {
	public static final byte DEFAULT_MESSAGES_TYPE = 0;
	public static final byte BOOT_MESSAGES_TYPE = 1;
	public static final byte LOOKUP_MESSAGES_TYPE = 2;
	public static final byte CHUNK_MESSAGES_TYPE = 3;
	public static final byte MIGRATION_MESSAGES_TYPE = 4;
	public static final byte LOCK_MESSAGES_TYPE = 5;
	public static final byte NAMESERVICE_MESSAGES_TYPE = 6;
	public static final byte LOG_MESSAGES_TYPE = 7;
	public static final byte FAILURE_MESSAGES_TYPE = 8;
	public static final byte RECOVERY_MESSAGES_TYPE = 9;
	public static final byte LOGGER_MESSAGES_TYPE = 10;
}
