
package de.uniduesseldorf.dxram.core.api;

import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Wrapper class for a ChunkID
 * @author Florian Klein
 *         23.07.2013
 */
public final class ChunkID {

	// Constants
	public static final long INVALID_ID = -1;

	public static final long MAX_ID = 281474976710655L;

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

		return (short)((p_chunkID & 0xFFFF000000000000L) >> 48);
	}

	/**
	 * Get the LocalID part of the ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the LocalID part
	 */
	public static long getLocalID(final long p_chunkID) {
		check(p_chunkID);

		return p_chunkID & 0x0000FFFFFFFFFFFFL;
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
		for (long chunkID : p_chunkIDs) {
			Contract.check(chunkID != INVALID_ID, "invalid ChunkID");
		}
	}

}
