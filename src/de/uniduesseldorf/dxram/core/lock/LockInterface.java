
package de.uniduesseldorf.dxram.core.lock;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.ChunkException;

/**
 * Methods for accessing the Lock-Service
 * @author Florian Klein
 *         06.08.2012
 */
public interface LockInterface extends CoreComponent {

	// Methods
	/**
	 * Releases all locks
	 * @throws ChunkException
	 *             if the locks could not be released
	 */
	void releaseAll() throws ChunkException;

	/**
	 * Checks if the Chunk for the given ChunkID is locked
	 * @param p_chunkID
	 *            the ChunkID
	 * @return true if the Chunk is locked, false otherwise
	 */
	boolean isLocked(long p_chunkID);

	/**
	 * Adds the given lock
	 * @param p_lock
	 *            the lock
	 * @throws ChunkException
	 *             if the lock could not be added
	 */
	void lock(AbstractLock p_lock) throws ChunkException;

	/**
	 * Releases the current lock for the given ChunkID and NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws ChunkException
	 *             if the lock could not be released
	 */
	void release(long p_chunkID, short p_nodeID) throws ChunkException;

	/**
	 * Releases the locks for the given ChunkIDs and NodeID
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 * @param p_nodeID
	 *            the NodeID
	 * @throws ChunkException
	 *             if the locks could not be released
	 */
	void release(long[] p_chunkIDs, short p_nodeID) throws ChunkException;

	/**
	 * Releases all locks for the given ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws ChunkException
	 *             if the locks could not be released
	 */
	void releaseAllByChunkID(long p_chunkID) throws ChunkException;

	/**
	 * Releases the locks for the given NodeID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws ChunkException
	 *             if the locks could not be released
	 */
	void releaseAllByNodeID(short p_nodeID) throws ChunkException;

	// Classes
	/**
	 * Represents a Lock for a Chunk
	 * @author Florian Klein
	 *         23.07.2013
	 */
	public abstract static class AbstractLock {

		// Attributes
		private long m_chunkID;
		private short m_nodeID;

		private AbstractLock m_next;

		// Constructors
		/**
		 * Creates an instance of AbstractLock
		 * @param p_chunkID
		 *            the ChunkID
		 * @param p_nodeID
		 *            the NodeID
		 */
		public AbstractLock(final long p_chunkID, final short p_nodeID) {
			ChunkID.check(p_chunkID);
			NodeID.check(p_nodeID);

			m_chunkID = p_chunkID;
			m_nodeID = p_nodeID;

			m_next = null;
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
		 * Gets the next lock
		 * @return the next lock
		 */
		public final AbstractLock getNext() {
			return m_next;
		}

		// Setters
		/**
		 * Sets the next lock
		 * @param p_next
		 *            the next lock
		 */
		public final void setNext(final AbstractLock p_next) {
			m_next = p_next;
		}

		// Methods
		/**
		 * Callback after the lock is enqueued
		 */
		public void enqueued() {}

		/**
		 * Callback after the lock is released
		 * @param p_chunk
		 *            the locked Chunk
		 */
		public abstract void release(Chunk p_chunk);

		/**
		 * Checks if a further lock exists
		 * @return true if a further lock exists, false otherwise
		 */
		public final boolean hasNext() {
			return m_next != null;
		}

		// Methods
		@Override
		public final String toString() {
			return this.getClass().getSimpleName() + "[" + m_chunkID + ", " + m_nodeID + ", " + hasNext() + "]";
		}

	}

}
