
package de.uniduesseldorf.dxram.core.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Implements the Lock-Service
 * @author Florian Klein 06.08.2012
 */
public final class DefaultLockHandler implements LockInterface {

	// Attributes
	private Map<Long, DefaultLock> m_locks;
	private boolean m_directResult;

	private Lock m_lock;

	// Constructors
	/**
	 * Creates an instance of LockHandler
	 */
	public DefaultLockHandler() {
		m_locks = null;
		m_directResult = false;

		m_lock = null;
	}

	// Getters
	/**
	 * Checks if the lock method should directly give a result
	 * @return true if the lock method should directly give a result, false otherwise
	 */
	public boolean isDirectResult() {
		return m_directResult;
	}

	// Setters
	/**
	 * Sets the direct result option
	 * @param p_directResult
	 *            if true the lock method will directly give a result
	 */
	public void setDirectResult(final boolean p_directResult) {
		m_directResult = p_directResult;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		m_locks = new HashMap<Long, DefaultLock>();

		m_lock = new ReentrantLock();
	}

	@Override
	public void close() {
		m_locks = null;

		m_lock = null;
	}

	@Override
	public void lock(final DefaultLock p_lock) throws MemoryException {
		DefaultLock lock;
		DefaultLock temp;

		Contract.checkNotNull(p_lock, "no lock given");

		try {
			m_lock.lock();

			p_lock.setEnqueued();

			lock = m_locks.get(p_lock.getChunkID());
			if (lock == null) {
				m_locks.put(p_lock.getChunkID(), p_lock);

				release(p_lock);
			} else {
				if (m_directResult) {
					p_lock.setChunk(Chunk.INVALID_CHUNKID);
					p_lock.setReleased();
				} else {
					temp = lock.getNext();
					while (temp != null) {
						lock = temp;
						temp = lock.getNext();
					}
					lock.setNext(p_lock);
				}
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlock(final long p_chunkID, final short p_nodeID) throws MemoryException {
		DefaultLock lock;

		ChunkID.check(p_chunkID);
		NodeID.check(p_nodeID);

		try {
			m_lock.lock();

			lock = m_locks.get(p_chunkID);
			if (lock != null && lock.getNodeID() == p_nodeID) {
				lock.setRemoved();

				m_locks.put(p_chunkID, lock.getNext());

				if (lock.getNext() != null) {
					release(lock.getNext());
				}
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlockAll(final long p_chunkID) {
		DefaultLock lock;

		ChunkID.check(p_chunkID);

		try {
			m_lock.lock();

			lock = m_locks.get(p_chunkID);
			while (lock != null) {
				lock.setRemoved();

				lock = lock.getNext();
			}
			m_locks.remove(p_chunkID);
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlockAll(final short p_nodeID) throws MemoryException {
		try {
			m_lock.lock();

			for (long l : m_locks.keySet()) {
				unlock(l, p_nodeID);
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlockAll() {
		try {
			m_lock.lock();

			for (long l : m_locks.keySet()) {
				unlockAll(l);
			}
		} finally {
			m_lock.unlock();
		}
	}

	/**
	 * Releases a lock
	 * @param p_lock
	 *            the lock
	 * @throws MemoryException
	 *             if the chunk could not be get
	 */
	private void release(final DefaultLock p_lock) throws MemoryException {
		Chunk chunk;

//		chunk = MemoryManager.get(p_lock.getChunkID());
//		p_lock.setChunk(chunk);
		p_lock.setReleased();
	}

	/**
	 * Prints debug infos
	 */
	public void printDebugInfos() {
		StringBuffer out;
		DefaultLock lock;

		out = new StringBuffer();

		m_lock.lock();

		out.append("\nLocked ID Count: " + m_locks.size());
		for (Entry<Long, DefaultLock> entry : m_locks.entrySet()) {
			out.append("\n" + Long.toHexString(entry.getKey()) + ":");

			lock = entry.getValue();
			while (lock != null) {
				out.append("\n\t\t" + lock);
				lock = lock.getNext();
			}
		}
		out.append("\n");

		m_lock.unlock();

		System.out.println(out);
	}

}
