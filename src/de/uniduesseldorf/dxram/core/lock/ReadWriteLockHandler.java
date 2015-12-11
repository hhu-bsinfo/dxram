
package de.uniduesseldorf.dxram.core.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Implements the Lock-Service using read-write locks
 * @author Florian Klein 01.02.2015
 */
public final class ReadWriteLockHandler implements LockInterface {

	// Attributes
	private Map<Long, DefaultLock> m_locks;
	private ChunkInterface m_chunk;

	private Lock m_lock;

	// Constructors
	/**
	 * Creates an instance of ReadWriteLockHandler
	 */
	public ReadWriteLockHandler() {
		m_locks = null;

		m_lock = null;
		try {
			m_chunk = CoreComponentFactory.getChunkInterface();
		} catch (final DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Methods
	@Override
	public void initialize() {
		m_locks = new HashMap<>();

		m_lock = new ReentrantLock(false);
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
		boolean release;

		Contract.checkNotNull(p_lock, "no lock given");

		try {
			m_lock.lock();

			p_lock.setEnqueued();
			release = false;

			lock = m_locks.get(p_lock.getChunkID());
			if (lock == null) {
				m_locks.put(p_lock.getChunkID(), p_lock);

				release = true;
			} else {
				release = lock.isReadLock() && p_lock.isReadLock();

				temp = lock.getNext();
				while (temp != null) {
					release &= temp.isReadLock();

					lock = temp;
					temp = lock.getNext();
				}
				lock.setNext(p_lock);
			}

			if (release) {
				release(p_lock);
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlock(final long p_chunkID, final short p_nodeID) throws MemoryException {
		DefaultLock lock;
		DefaultLock previous;
		DefaultLock next;
		DefaultLock newFirst;

		try {
			m_lock.lock();

			// Remove locks
			lock = m_locks.get(p_chunkID);
			previous = null;
			next = null;
			newFirst = null;
			while (lock != null) {
				next = lock.getNext();
				if (lock.getNodeID() == p_nodeID) {
					lock.setRemoved();

					if (newFirst == null || newFirst.getNodeID() == p_nodeID) {
						newFirst = next;
					}

					if (previous != null) {
						previous.setNext(next);
					}
				} else {
					previous = lock;
				}

				lock = next;
			}
			m_locks.put(p_chunkID, newFirst);

			// Release next lock(s)
			lock = m_locks.get(p_chunkID);
			if (lock != null) {
				System.out.println("Remaining Lock: " + lock);
				if (!lock.isReleased()) {
					release(lock);

					if (lock.isReadLock()) {
						lock = lock.getNext();
						while (lock != null && lock.isReadLock()) {
							release(lock);

							lock = lock.getNext();
						}
					}
				}
			} else {
				m_locks.remove(p_chunkID);
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void unlockAll(final long p_chunkID) {
		DefaultLock lock;

		try {
			m_lock.lock();

			lock = m_locks.remove(p_chunkID);
			while (lock != null) {
				lock.setRemoved();

				lock = lock.getNext();
			}
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
		int size;
		int bytesRead;
		MemoryManager memMan;

		memMan = m_chunk.getMemoryManager();

		size = memMan.getSize(p_lock.getChunkID());
		chunk = new Chunk(p_lock.getChunkID(), size);
		bytesRead = memMan.get(p_lock.getChunkID(), chunk.getData().array(), 0, size);

		p_lock.setChunk(chunk);
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
