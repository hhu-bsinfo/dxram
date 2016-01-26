
package de.hhu.bsinfo.dxram.data;

/**
 * Helper class for ChunkID related issues. 
 *
 * @author Florian Klein
 *         23.07.2013
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class ChunkID {

	public static final long INVALID_ID = -1;
	public static final long CREATORID_BITMASK = 0xFFFF000000000000L;
	public static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

	public static final long MAX_LOCALID = Long.MAX_VALUE & LOCALID_BITMASK;

	/**
	 * Static class.
	 */
	private ChunkID() {}

	/**
	 * Get the CreatorID/NodeID part of the ChunkID.
	 * @param p_chunkID ChunkID.
	 * @return The NodeID/CreatorID part.
	 */
	public static short getCreatorID(final long p_chunkID) {
		assert p_chunkID != INVALID_ID;

		return (short) ((p_chunkID & CREATORID_BITMASK) >> 48);
	}

	/**
	 * Get the LocalID part of the ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the LocalID part
	 */
	public static long getLocalID(final long p_chunkID) {
		assert p_chunkID != INVALID_ID;

		return p_chunkID & LOCALID_BITMASK;
	}
}
