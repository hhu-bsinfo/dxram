
package de.uniduesseldorf.dxram.core.chunk;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Methods for accessing the data
 * @author Florian Klein
 *         09.03.2012
 */
public interface ChunkInterface extends CoreComponent {

	// Methods
	/**
	 * Set the IncomingChunkListener
	 * @param p_listener
	 *            the IncomingChunkListener
	 */
	void setListener(final IncomingChunkListener p_listener);

	long create(final int p_size) throws DXRAMException;
	
	long[] create(final int[] p_sizes) throws DXRAMException;

	long create(final int p_size, final int p_id) throws DXRAMException;

	/**
	 * Creates multiple new Chunks with identifier
	 * @param p_sizes
	 *            the sizes of the new Chunks
	 * @param p_id
	 *            the id of the first Chunk
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be created
	 */
	long[] create(final int[] p_sizes, final int p_id) throws DXRAMException;

	
	int readVersion(final long p_chunkID) throws DXRAMException;
	
	int readVersion(final int p_id) throws DXRAMException;
	
	int readPayloadSize(final long p_chunkID) throws DXRAMException;
	
	int readPayloadSize(final int p_id) throws DXRAMException;
	
	int readPayload(final long p_chunkID, final byte[] p_buffer) throws DXRAMException;
	
	int readPayload(final long p_chunkID, final int p_payloadOffset, final int p_length, final int p_bufferOffset, final byte[] p_buffer) throws DXRAMException;

	/**
	 * Get the corresponding ChunkID for the given identifier
	 * @param p_id
	 *            the id
	 * @return the corresponding ChunkID
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	long getChunkID(final int p_id) throws DXRAMException;

	/**
	 * Request the corresponding Chunk for the given ID<br>
	 * An IncomingChunkEvent will be triggered on the arrival of the Chunk
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	void getAsync(final long p_chunkID) throws DXRAMException;


	int writePayload(final long p_chunkID, final byte[] p_buffer) throws DXRAMException;
	
	int writePayload(final long p_chunkID, final int p_payloadOffset, final int p_length, final int p_bufferOffset, final byte[] p_buffer) throws DXRAMException;

	/**
	 * Removes the corresponding Chunk for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be removed
	 */
	void remove(final long p_chunkID) throws DXRAMException;

	/**
	 * Removes the corresponding Chunks for the given IDs
	 * @param p_chunkIDs
	 *            the IDs
	 * @throws DXRAMException
	 *             if the Chunks could not be removed
	 */
	void remove(final long[] p_chunkIDs) throws DXRAMException;

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID
	 * @return the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be locked
	 */
	boolean lock(final long p_chunkID) throws DXRAMException;

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID
	 * @param p_readLock
	 *            true if the lock is a read lock, false otherwise
	 * @return the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be locked
	 */
	boolean lock(final long p_chunkID, final boolean p_readLock) throws DXRAMException;

	/**
	 * Unlocks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be unlocked
	 */
	boolean unlock(long p_chunkID) throws DXRAMException;

//	/**
//	 * Puts recovered Chunks
//	 * @param p_chunks
//	 *            the Chunks
//	 * @throws DXRAMException
//	 *             if the Chunks could not be put
//	 */
//	void putRecoveredChunks(Chunk[] p_chunks) throws DXRAMException;

	/**
	 * Migrates the corresponding Chunk for the giving ID to another Node
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @return true=success, false=failed
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	boolean migrate(long p_chunkID, short p_target) throws DXRAMException;

	/**
	 * Migrates the corresponding Chunks for the giving ID range to another Node
	 * @param p_startChunkID
	 *            the first ID
	 * @param p_endChunkID
	 *            the last ID
	 * @param p_target
	 *            the Node where to migrate the Chunks
	 * @return true=success, false=failed
	 * @throws DXRAMException
	 *             if the Chunks could not be migrated
	 */
	boolean migrateRange(long p_startChunkID, long p_endChunkID, short p_target) throws DXRAMException;

	/**
	 * Migrates all chunks to another node. Is called for promotion.
	 * @param p_target
	 *            the peer that should take over all chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be migrated
	 */
	void migrateAll(short p_target) throws DXRAMException;

}
