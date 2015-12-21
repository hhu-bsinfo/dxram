package de.uniduesseldorf.dxram.core.lock;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

public abstract class LockComponent extends DXRAMComponent {

	public static final String COMPONENT_IDENTIFIER = "Lock";
	
	public LockComponent(int p_priorityInit, int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);
	}

	// Methods
	/**
	 * Locks a Chunk
	 * @param p_lock
	 *            the lock
	 * @throws DXRAMException
	 *             if the Chunk could not be locked
	 */
	public abstract void lock(DefaultLock p_lock);

	/**
	 * Unlocks a Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunk could not be unlocked
	 */
	public abstract void unlock(long p_chunkID, short p_nodeID);

	/**
	 * Unlocks a Chunk (all locks)
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	public abstract void unlockAll(long p_chunkID);

	/**
	 * Unlocks all Chunks (locked by a node)
	 * @param p_nodeID
	 *            the NodeID
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	public abstract void unlockAll(short p_nodeID);

	/**
	 * Unlocks all Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be unlocked
	 */
	public abstract void unlockAll();
}
