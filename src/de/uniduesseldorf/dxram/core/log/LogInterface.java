
package de.uniduesseldorf.dxram.core.log;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.mem.Chunk;

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
	 * Recovers all Chunks of given backup range
	 * @param p_fileName
	 *            the file name
	 * @param p_path
	 *            the path of the folder the file is in
	 * @return the recovered Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	Chunk[] recoverBackupRangeFromFile(String p_fileName, String p_path) throws DXRAMException;

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
	 * @return the header size
	 */
	short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size);
}
