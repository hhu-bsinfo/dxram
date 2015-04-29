
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
	void setListener(IncomingChunkListener p_listener);

	/**
	 * Creates a new Chunk
	 * @param p_size
	 *            the size of the new Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be created
	 */
	Chunk create(int p_size) throws DXRAMException;

	/**
	 * Creates multiple new Chunks
	 * @param p_sizes
	 *            the sizes of the new Chunks
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be created
	 */
	Chunk[] create(int[] p_sizes) throws DXRAMException;

	/**
	 * Creates a new Chunk with identifier
	 * @param p_size
	 *            the size of the new Chunk
	 * @param p_id
	 *            the id of the new Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be created
	 */
	Chunk create(int p_size, int p_id) throws DXRAMException;

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
	Chunk[] create(int[] p_sizes, int p_id) throws DXRAMException;

	/**
	 * Get the corresponding Chunk for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @return the corresponding Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	Chunk get(long p_chunkID) throws DXRAMException;

	/**
	 * Get the corresponding Chunks for the given IDs
	 * @param p_chunkIDs
	 *            the IDs
	 * @return the corresponding Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be get
	 */
	Chunk[] get(long[] p_chunkIDs) throws DXRAMException;

	/**
	 * Get the corresponding Chunk for the given identifier
	 * @param p_id
	 *            the id
	 * @return the corresponding Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	Chunk get(int p_id) throws DXRAMException;

	/**
	 * Get the corresponding ChunkID for the given identifier
	 * @param p_id
	 *            the id
	 * @return the corresponding ChunkID
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	long getChunkID(int p_id) throws DXRAMException;

	/**
	 * Request the corresponding Chunk for the given ID<br>
	 * An IncomingChunkEvent will be triggered on the arrival of the Chunk
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be get
	 */
	void getAsync(long p_chunkID) throws DXRAMException;

	/**
	 * Updates the given Chunk
	 * @param p_chunk
	 *            the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be put
	 */
	void put(Chunk p_chunk) throws DXRAMException;

	/**
	 * Removes the corresponding Chunk for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be removed
	 */
	void remove(long p_chunkID) throws DXRAMException;

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID
	 * @return the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be locked
	 */
	Chunk lock(long p_chunkID) throws DXRAMException;

	/**
	 * Unlocks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID
	 * @throws DXRAMException
	 *             if the Chunk could not be unlocked
	 */
	void unlock(long p_chunkID) throws DXRAMException;

	/**
	 * Migrates the corresponding Chunk for the giving ID to another Node
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	void migrate(long p_chunkID, short p_target) throws DXRAMException;

	/**
	 * Migrates the corresponding Chunks for the giving ID range to another Node
	 * @param p_startChunkID
	 *            the first ID
	 * @param p_endChunkID
	 *            the last ID
	 * @param p_target
	 *            the Node where to migrate the Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be migrated
	 */
	void migrateRange(long p_startChunkID, long p_endChunkID, short p_target) throws DXRAMException;

	/**
	 * Migrates all chunks to another node. Is called for promotion.
	 * @param p_target
	 *            the peer that should take over all chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be migrated
	 */
	void migrateAll(short p_target) throws DXRAMException;

	/**
	 * Recover the local data from the log
	 * @throws DXRAMException
	 *             if the Chunks could not be recoverd
	 */
	void recoverFromLog() throws DXRAMException;

}
