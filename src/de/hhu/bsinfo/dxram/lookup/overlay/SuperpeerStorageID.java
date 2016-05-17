package de.hhu.bsinfo.dxram.lookup.overlay;

/**
 * Helper for handling superpeer storage IDs.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.16
 */
public class SuperpeerStorageID {
	public static final long INVALID_ID = -1;
	private static final long CREATORID_BITMASK = 0xFFFF000000000000L;

	/**
	 * Static class
	 */
	private SuperpeerStorageID() {
	}

	/**
	 * Get the superpeer ID part of the storage id.
	 *
	 * @param p_storageId Storage id.
	 * @return The superpeer node id part.
	 */
	public static short getSuperpeerID(final long p_storageId) {
		assert p_storageId != INVALID_ID;

		return (short) ((p_storageId & CREATORID_BITMASK) >> 48);
	}

	/**
	 * Get the LocalID part of the super peer storage id
	 *
	 * @param p_storageId Storage id.
	 * @return the LocalID part
	 */
	public static int getLocalID(final long p_storageId) {
		assert p_storageId != INVALID_ID;

		return (int) p_storageId;
	}

	/**
	 * Create a full superpeer storage id from a local and superpeer node ID.
	 *
	 * @param p_nid Node ID part.
	 * @param p_lid Local ID part.
	 * @return Full superpeer storage id
	 */
	public static long getStorageId(final short p_nid, final long p_lid) {
		return (((long) p_nid) << 48) | p_lid;
	}

	/**
	 * Convert a superpeer storage id to a hex string
	 *
	 * @param p_storageId Storage id to convert to a hex string.
	 * @return Converted storage id, example: 0x1111000000000001
	 */
	public static String toHexString(final long p_storageId) {
		return "0x" + Long.toHexString(p_storageId).toUpperCase();
	}
}
