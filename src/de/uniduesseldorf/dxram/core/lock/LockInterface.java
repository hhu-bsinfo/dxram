
package de.uniduesseldorf.dxram.core.lock;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Methods for accessing the Lock-Service
 * @author Florian Klein
 *         06.08.2012
 */
public interface LockInterface extends CoreComponent {

	// Methods
	/**
	 * Locks a Chunk
	 * @param p_lock
	 *            the lock
	 * @throws DXRAMException
	 *             if the Chunk could not be locked
	 */
	void lock(DefaultLock p_lock) throws DXRAMException;

	/**
	 * Unlocks a Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunk could not be unlocked
	 */
	void unlock(long p_chunkID, short p_nodeID) throws DXRAMException;

	/**
	 * Unlocks a Chunk (all locks)
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	void unlockAll(long p_chunkID) throws DXRAMException;

	/**
	 * Unlocks all Chunks (locked by a node)
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	void unlockAll(short p_nodeID) throws DXRAMException;

	/**
	 * Unlocks all Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	void unlockAll() throws DXRAMException;

}
