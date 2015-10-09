
package de.uniduesseldorf.dxram.core.log;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Methods for logging the data
 * @author Kevin Beineke 23.05.2014
 */
public interface LogInterface extends CoreComponent {

	/**
	 * Initializes a backup range for Chunks created on this node
	 * @Note: Test method to stress logging
	 */
	// TODO: Remove
	void initBackupRangeLocallyTEST();

	/**
	 * Logs a Chunk locally (created on the very same node)
	 * @param p_chunk
	 *            the Chunk
	 * @throws DXRAMException
	 *             if the Chunks could not be logged
	 * @Note: Test method to stress logging
	 */
	// TODO: Remove
	void logChunkLocallyTEST(final Chunk p_chunk) throws DXRAMException;

	/**
	 * Removes a Chunk locally (created on the very same node)
	 * @param p_chunkID
	 *            the Chunk
	 * @param p_version
	 *            the version
	 * @throws DXRAMException
	 *             if the Chunks could not be removed
	 * @Note: Test method to stress logging
	 */
	// TODO: Remove
	void removeChunkLocallyTEST(final long p_chunkID, final int p_version) throws DXRAMException;

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
	 * Recovers all Chunks of given backup range
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @return the recovered Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	Chunk[] recoverBackupRange(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException;

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
	void printBackupRange(short p_owner, long p_chunkID, byte p_rangeID) throws DXRAMException;

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
