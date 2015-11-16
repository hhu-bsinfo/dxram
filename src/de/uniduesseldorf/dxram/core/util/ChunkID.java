
package de.uniduesseldorf.dxram.core.util;

import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Wrapper class for a ChunkID
 * @author Florian Klein
 *         23.07.2013
 */
public final class ChunkID {

	// Constants
	public static final long INVALID_ID = -1;
	private static final long CREATORID_BITMASK = 0xFFFF000000000000L;
	private static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

	public static final long MAX_LOCALID = Long.MAX_VALUE & LOCALID_BITMASK;

	// Constructors
	/**
	 * Creates an instance of ChunkID
	 */
	private ChunkID() {}

	// Methods
	/**
	 * Get the CreatorID part of the ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the CreatorID part
	 */
	public static short getCreatorID(final long p_chunkID) {
		check(p_chunkID);

		return (short) ((p_chunkID & CREATORID_BITMASK) >> 48);
	}

	/**
	 * Get the LocalID part of the ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the LocalID part
	 */
	public static long getLocalID(final long p_chunkID) {
		check(p_chunkID);

		return p_chunkID & LOCALID_BITMASK;
	}

	/**
	 * Checks if the ChunkID is valid
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public static void check(final long p_chunkID) {
		Contract.check(p_chunkID != INVALID_ID, "invalid ChunkID");
	}

	/**
	 * Checks if the ChunkIDs is valid
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 */
	public static void check(final long[] p_chunkIDs) {
		for (final long chunkID : p_chunkIDs) {
			Contract.check(chunkID != INVALID_ID, "invalid ChunkID");
		}
	}

}
