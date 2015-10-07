
package de.uniduesseldorf.dxram.core.log;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Methods for logging the data
 * @author Kevin Beineke 23.05.2014
 */
public interface LogInterface extends CoreComponent {

	// Methods
	/**
	 * Initializes a new backup range
	 * @param p_firstChunkIDOrRangeID
	 *            the beginning of the range
	 * @param p_backupPeers
	 *            the backup peers
	 */
	void initBackupRange(long p_firstChunkIDOrRangeID, short[] p_backupPeers);

	/**
	 * Recovers the local data of one backup range
	 * @param p_owner
	 *            the NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	void recoverAllLogEntries(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException;

	/**
	 * Recovers some local data of one node from log
	 * @param p_owner
	 *            the NodeID
	 * @param p_low
	 *            lower bound
	 * @param p_high
	 *            higher bound
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	void recoverRange(short p_owner, long p_low, long p_high) throws DXRAMException;

	/**
	 * Reads the local data of one log
	 * @param p_owner
	 *            the NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @return the local data
	 * @note for testing only
	 */
	byte[][] readAllEntries(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException;

	/**
	 * Prints the metadata of one node's log
	 * @param p_owner
	 *            the NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @note for testing only
	 */
	void printMetadataOfAllEntries(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException;

	/**
	 * Returns the header size
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size of the Chunk
	 * @param p_version
	 *            the version of the Chunk
	 * @return the header size
	 */
	short getHeaderSize(final short p_nodeID, final long p_localID, final int p_size, final int p_version);
}
