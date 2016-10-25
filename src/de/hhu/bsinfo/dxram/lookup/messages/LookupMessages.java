
package de.hhu.bsinfo.dxram.lookup.messages;

/**
 * Type and list of subtypes for all lookup messages
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public final class LookupMessages {

	// Constants
	public static final byte SUBTYPE_JOIN_REQUEST = 1;
	public static final byte SUBTYPE_JOIN_RESPONSE = 2;
	public static final byte SUBTYPE_GET_LOOKUP_RANGE_REQUEST = 3;
	public static final byte SUBTYPE_GET_LOOKUP_RANGE_RESPONSE = 4;
	public static final byte SUBTYPE_REMOVE_CHUNKIDS_REQUEST = 5;
	public static final byte SUBTYPE_REMOVE_CHUNKIDS_RESPONSE = 6;
	public static final byte SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST = 7;
	public static final byte SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE = 8;
	public static final byte SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST = 9;
	public static final byte SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE = 10;
	public static final byte SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST = 11;
	public static final byte SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE = 12;
	public static final byte SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST = 13;
	public static final byte SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE = 14;
	public static final byte SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE = 15;
	public static final byte SUBTYPE_MIGRATE_REQUEST = 16;
	public static final byte SUBTYPE_MIGRATE_RESPONSE = 17;
	public static final byte SUBTYPE_MIGRATE_RANGE_REQUEST = 18;
	public static final byte SUBTYPE_MIGRATE_RANGE_RESPONSE = 19;
	public static final byte SUBTYPE_INIT_RANGE_REQUEST = 20;
	public static final byte SUBTYPE_INIT_RANGE_RESPONSE = 21;
	public static final byte SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST = 22;
	public static final byte SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE = 23;
	public static final byte SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE = 24;
	public static final byte SUBTYPE_PING_SUPERPEER_MESSAGE = 25;

	public static final byte SUBTYPE_SEND_BACKUPS_MESSAGE = 26;
	public static final byte SUBTYPE_SEND_SUPERPEERS_MESSAGE = 27;
	public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST = 28;
	public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE = 29;
	public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST = 30;
	public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE = 31;
	public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE = 32;
	public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE = 33;

	public static final byte SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_REQUEST = 34;
	public static final byte SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_RESPONSE = 35;
	public static final byte SUBTYPE_START_RECOVERY_MESSAGE = 36;
	public static final byte SUBTYPE_REPLACE_BACKUP_PEER_REQUEST = 37;
	public static final byte SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE = 38;

	public static final byte SUBTYPE_BARRIER_ALLOC_REQUEST = 39;
	public static final byte SUBTYPE_BARRIER_ALLOC_RESPONSE = 40;
	public static final byte SUBTYPE_BARRIER_FREE_REQUEST = 41;
	public static final byte SUBTYPE_BARRIER_FREE_RESPONSE = 42;
	public static final byte SUBTYPE_BARRIER_SIGN_ON_REQUEST = 43;
	public static final byte SUBTYPE_BARRIER_SIGN_ON_RESPONSE = 44;
	public static final byte SUBTYPE_BARRIER_RELEASE_MESSAGE = 45;
	public static final byte SUBTYPE_BARRIER_STATUS_REQUEST = 46;
	public static final byte SUBTYPE_BARRIER_STATUS_RESPONSE = 47;
	public static final byte SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST = 48;
	public static final byte SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE = 49;

	public static final byte SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST = 50;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_CREATE_RESPONSE = 51;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST = 52;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE = 53;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST = 54;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE = 55;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE = 56;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST = 57;
	public static final byte SUBTYPE_SUPERPEER_STORAGE_STATUS_RESPONSE = 58;

	public static final byte SUBTYPE_GET_LOOKUP_TREE_REQUEST = 59;
	public static final byte SUBTYPE_GET_LOOKUP_TREE_RESPONSE = 60;
	public static final byte SUBTYPE_GET_METADATA_SUMMARY_REQUEST = 61;
	public static final byte SUBTYPE_GET_METADATA_SUMMARY_RESPONSE = 62;

	/**
	 * Hidden constructor
	 */
	private LookupMessages() {}
}
