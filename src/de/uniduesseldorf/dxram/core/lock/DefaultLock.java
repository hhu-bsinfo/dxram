
package de.uniduesseldorf.dxram.core.lock;

import java.util.concurrent.Semaphore;

import de.uniduesseldorf.dxram.core.chunk.Chunk;

/**
 * Represents a Lock
 * @author klein 26.03.2015
 */
public class DefaultLock {

	// Attributes
	private long m_chunkID;
	private short m_nodeID;
	private Chunk m_chunk;

	private boolean m_enqueued;
	private volatile boolean m_released;
	private volatile boolean m_removed;

	private boolean m_readLock;
	private DefaultLock m_next;

	private Semaphore m_waiting;

	// Constructors
	/**
	 * Creates an instance of DefaultLock
	 * @param p_chunkID
	 *            the ChunkID to lock
	 * @param p_nodeID
	 *            the requesting NodeID
	 */
	public DefaultLock(final long p_chunkID, final short p_nodeID) {
		this(p_chunkID, p_nodeID, false);
	}

	/**
	 * Creates an instance of DefaultLock
	 * @param p_chunkID
	 *            the ChunkID to lock
	 * @param p_nodeID
	 *            the requesting NodeID
	 * @param p_readLock
	 *            if true the lock is a read lock, otherwise it is a write lock
	 */
	public DefaultLock(final long p_chunkID, final short p_nodeID, final boolean p_readLock) {
		m_chunkID = p_chunkID;
		m_nodeID = p_nodeID;
		m_chunk = null;

		m_enqueued = false;
		m_released = false;
		m_removed = false;

		m_readLock = p_readLock;
		m_next = null;

		m_waiting = new Semaphore(0, false);
	}

	// Getters
	/**
	 * Gets the ChunkID
	 * @return the ChunkID
	 */
	public final long getChunkID() {
		return m_chunkID;
	}

	/**
	 * Gets the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
	}

	/**
	 * Gets the Chunk
	 * @return the Chunk
	 */
	public final Chunk getChunk() {
		while (!m_released && !m_removed) {
			try {
				m_waiting.acquire();
			} catch (final InterruptedException e) {}
		}

		return m_chunk;
	}

	/**
	 * Checks if the lock is enqueued
	 * @return true if the lock is enqueued, false otherwise
	 */
	public final boolean isEnqueued() {
		return m_enqueued;
	}

	/**
	 * Checks if the lock is released
	 * @return true if the lock is released, false otherwise
	 */
	public final boolean isReleased() {
		return m_released;
	}

	/**
	 * Checks if the lock is removed
	 * @return true if the lock is removed, false otherwise
	 */
	public final boolean isRemoved() {
		return m_removed;
	}

	/**
	 * Checks if the lock is a read lock
	 * @return true if the lock is a read lock, otherwise it is a write lock
	 */
	public final boolean isReadLock() {
		return m_readLock;
	}

	/**
	 * Gets the next lock
	 * @return the next lock
	 */
	final DefaultLock getNext() {
		return m_next;
	}

	// Setters
	/**
	 * Sets the Chunk
	 * @param p_chunk
	 *            the CHunk
	 */
	final void setChunk(final Chunk p_chunk) {
		m_chunk = p_chunk;
	}

	/**
	 * Sets the next lock
	 * @param p_next
	 *            the next lock
	 */
	final void setNext(final DefaultLock p_next) {
		m_next = p_next;
	}

	// Methods
	/**
	 * Marks the lock as enqueued
	 */
	final void setEnqueued() {
		m_enqueued = true;
	}

	/**
	 * Marks the lock as released
	 */
	final void setReleased() {
		m_released = true;

		m_waiting.release();
	}

	/**
	 * Marks the lock as removed
	 */
	final void setRemoved() {
		m_removed = true;

		m_waiting.release();
	}

}
