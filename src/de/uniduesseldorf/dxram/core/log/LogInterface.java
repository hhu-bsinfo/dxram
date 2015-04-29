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
	 * Returns the secondary log buffer
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_set
	 *            whether to create it if null or not
	 * @return the secondary log buffer
	 * @throws IOException
	 *             if the secondary log buffer could not be returned
	 * @throws InterruptedException
	 *             if the secondary log buffer could not be returned
	 */
	SecondaryLogBuffer getSecondaryLogBuffer(short p_nodeID, boolean p_set)
			throws IOException, InterruptedException;


	/**
	 * Returns the secondary log
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_set
	 *            whether to create it if null or not
	 * @return the secondary log
	 * @throws IOException
	 *             if the secondary log could not be returned
	 * @throws InterruptedException
	 *             if the secondary log could not be returned
	 */
	SecondaryLogWithSegments getSecondaryLog(short p_nodeID, boolean p_set)
			throws IOException, InterruptedException;


	// Methods
	/**
	 * Creates a new Chunk
	 * @param p_chunk
	 *            the chunk
	 * @return number of successfully written bytes
	 * @throws DXRAMException
	 *             if the Chunk could not be logged
	 */
	long logChunk(Chunk p_chunk) throws DXRAMException;


	/**
	 * Creates a new Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws DXRAMException
	 *             if the Chunk could not be logged
	 */
	void removeChunk(long p_chunkID) throws DXRAMException;


	/**
	 * Recovers the local data of one node from the log
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	void recoverAllLogEntries(short p_nodeID) throws DXRAMException;


	/**
	 * Recovers some local data of one node from the log
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_low
	 *            lower bound
	 * @param p_high
	 *            higher bound
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	void recoverRange(short p_nodeID, long p_low, long p_high)
			throws DXRAMException;


	/**
	 * Reads the local data of one node from the log
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_manipulateReadPtr
	 *            whether the read pointer should be adjusted or not
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @return the local data
	 * @note for testing only
	 */
	byte[][] readAllEntries(short p_nodeID, boolean p_manipulateReadPtr)
			throws DXRAMException;


	/**
	 * Prints the metadata of one node's data from the log
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @note for testing only
	 */
	void printMetadataOfAllEntries(short p_nodeID) throws DXRAMException;


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
