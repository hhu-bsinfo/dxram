package de.uniduesseldorf.dxram.core.lock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.exceptions.ChunkException;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Implements the Lock-Service
 * @author Florian Klein 06.08.2012
 */
public final class LockHandler implements LockInterface {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(LockHandler.class);

	// Attributes
	private Map<Long, AbstractLock> m_entries;
	private ReadWriteLock m_lock;

	// Constructors
	/**
	 * Creates an instance of LockHandler
	 */
	public LockHandler() {
		m_entries = new HashMap<Long, AbstractLock>();
		m_lock = new ReentrantReadWriteLock();
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {}

	/**
	 * Closes Component und frees unused ressources
	 */
	@Override
	public void close() {}

	@Override
	public void releaseAll() throws ChunkException {
		LOGGER.trace("Entering clear");

		m_lock.writeLock().lock();

		for (AbstractLock lock : m_entries.values()) {
			while (lock != null) {
				release(lock.getChunkID(), lock.getNodeID());

				lock = lock.getNext();
			}
		}
		m_entries.clear();

		m_lock.writeLock().unlock();

		LOGGER.trace("exiting clear");
	}

	@Override
	public boolean isLocked(final long p_chunkID) {
		boolean ret;

		LOGGER.trace("Entering isLocked with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		m_lock.readLock().lock();

		ret = m_entries.get(p_chunkID) != null;

		m_lock.readLock().unlock();

		LOGGER.trace("Exiting isLocked with: ret=" + ret);

		return ret;
	}

	@Override
	public void lock(final AbstractLock p_lock) throws ChunkException {
		AbstractLock lock;
		Chunk chunk = null;

		LOGGER.trace("Entering lock with: p_lock=" + p_lock);

		Contract.checkNotNull(p_lock, "no lock given");

		m_lock.writeLock().lock();

		lock = m_entries.get(p_lock.getChunkID());
		if (lock == null) {
			// No existing lock
			m_entries.put(p_lock.getChunkID(), p_lock);

			try {
				chunk = MemoryManager.get(p_lock.getChunkID());
			} catch (final MemoryException e) {
				throw new ChunkException("Could not get chunk", e);
			}

			LOGGER.debug("Lock released: " + p_lock + ", " + chunk);

			p_lock.release(chunk);
		} else {
			// Append lock
			while (lock.getNext() != null) {
				lock = lock.getNext();
			}
			lock.setNext(p_lock);

			m_lock.writeLock().unlock();
			p_lock.enqueued();
			m_lock.writeLock().lock();
		}

		m_lock.writeLock().unlock();

		LOGGER.trace("Exiting lock");
	}

	@Override
	public void release(final long p_chunkID, final short p_nodeID) throws ChunkException {
		AbstractLock lock;
		Chunk chunk;

		LOGGER.trace("Entering release with: p_chunkID=" + p_chunkID + ", p_nodeID=" + p_nodeID);

		ChunkID.check(p_chunkID);
		NodeID.check(p_nodeID);

		m_lock.writeLock().lock();

		lock = m_entries.get(p_chunkID);
		if (lock != null && lock.getNodeID() == p_nodeID) {
			m_entries.put(p_chunkID, lock.getNext());

			if (lock.getNext() != null) {
				try {
					chunk = MemoryManager.get(lock.getChunkID());
				} catch (final MemoryException e) {
					throw new ChunkException("Could not get chunk", e);
				}

				LOGGER.debug("Lock released: " + lock + ", " + chunk);

				lock.getNext().release(chunk);
			}
		}

		m_lock.writeLock().unlock();

		LOGGER.trace("Exiting release");
	}

	@Override
	public void release(final long[] p_chunkIDs, final short p_nodeID) throws ChunkException {
		LOGGER.trace("Entering release with: p_chunkIDs=" + Arrays.toString(p_chunkIDs) + ", p_nodeID=" + p_nodeID);

		Contract.checkNotNull(p_chunkIDs, "no chunkIDs given");
		NodeID.check(p_nodeID);

		m_lock.writeLock().lock();

		for (long chunkID : p_chunkIDs) {
			release(chunkID, p_nodeID);
		}

		m_lock.writeLock().unlock();

		LOGGER.trace("Exiting release");
	}

	@Override
	public void releaseAllByChunkID(final long p_chunkID) throws ChunkException {
		AbstractLock lock;

		LOGGER.trace("Entering releaseAllByChunkID with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		m_lock.writeLock().lock();

		lock = m_entries.get(p_chunkID);
		while (lock != null) {
			release(p_chunkID, lock.getNodeID());

			lock = lock.getNext();
		}
		m_entries.remove(p_chunkID);

		m_lock.writeLock().unlock();

		LOGGER.trace("Exiting releaseAllByChunkID");
	}

	@Override
	public void releaseAllByNodeID(final short p_nodeID) throws ChunkException {
		LOGGER.trace("Entering releaseAllByNodeID with: p_nodeID=" + p_nodeID);

		NodeID.check(p_nodeID);

		m_lock.writeLock().lock();

		for (AbstractLock lock : m_entries.values()) {
			if (lock != null) {
				while (lock.getNext() != null) {
					if (lock.getNext().getNodeID() == p_nodeID) {
						lock.setNext(lock.getNext().getNext());
					} else {
						lock = lock.getNext();
					}
				}

				release(lock.getChunkID(), p_nodeID);
			}
		}

		m_lock.writeLock().unlock();

		LOGGER.trace("Exiting releaseAllByNodeID");
	}

}
