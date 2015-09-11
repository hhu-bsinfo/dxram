
package de.uniduesseldorf.dxram.core.log;

import java.io.IOException;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryLog;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogWithSegments;

/**
 * Methods for logging the data
 * @author Kevin Beineke 23.05.2014
 */
public interface LogInterface extends CoreComponent {

	// Getter
	/**
	 * Returns the primary log
	 * @return the primary log
	 */
	PrimaryLog getPrimaryLog();

	/**
	 * Returns the secondary log
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_source
	 *            the source NodeID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log
	 * @throws IOException
	 *             if the secondary log could not be returned
	 * @throws InterruptedException
	 *             if the secondary log could not be returned
	 */
	SecondaryLogWithSegments getSecondaryLog(long p_chunkID, short p_source, byte p_rangeID) throws IOException, InterruptedException;

	/**
	 * Returns the secondary log buffer
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_source
	 *            the source NodeID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log buffer
	 * @throws IOException
	 *             if the secondary log buffer could not be returned
	 * @throws InterruptedException
	 *             if the secondary log buffer could not be returned
	 */
	SecondaryLogBuffer getSecondaryLogBuffer(long p_chunkID, short p_source, byte p_rangeID) throws IOException, InterruptedException;

	/**
	 * Returns the backup range
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the first ChunkID of the range
	 */
	long getBackupRange(long p_chunkID);

	/**
	 * Returns the header size
	 * @param p_nodeID
	 *            the NodeID
	 * @return the header size
	 */
	short getHeaderSize(final short p_nodeID);

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
	 * Creates a new Chunk
	 * @param p_chunk
	 *            the chunk
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_source
	 *            the source NodeID
	 * @return number of successfully written bytes
	 * @throws DXRAMException
	 *             if the Chunk could not be logged
	 */
	long logChunk(Chunk p_chunk, byte p_rangeID, short p_source) throws DXRAMException;

	/**
	 * Creates a new Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_source
	 *            the source NodeID
	 * @throws DXRAMException
	 *             if the Chunk could not be logged
	 */
	void removeChunk(long p_chunkID, byte p_rangeID, short p_source) throws DXRAMException;

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
	 * Flushes the primary log write buffer
	 * @throws IOException
	 *             if primary log could not be flushed
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	void flushDataToPrimaryLog() throws IOException, InterruptedException;

	/**
	 * Flushes all secondary log buffers
	 * @throws IOException
	 *             if at least one secondary log could not be flushed
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	void flushDataToSecondaryLogs() throws IOException, InterruptedException;

	/**
	 * Grants the reorganization thread access to a secondary log
	 */
	void grantAccess();
}
