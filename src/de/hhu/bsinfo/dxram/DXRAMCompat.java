package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.engine.DXRAMException;
import de.hhu.bsinfo.dxram.lock.LockService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.util.ChunkID;
import de.hhu.bsinfo.dxram.util.ChunkLockOperation;

/**
 * Wrapper to provide the old DXRAM Core API.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 25.01.16
 */
public class DXRAMCompat 
{	
	private DXRAM m_dxram = null;
	private ChunkService m_chunkService = null;
	private NameserviceService m_nameserviceService = null;
	private LockService m_lockService = null;
	
	public DXRAMCompat(final DXRAM p_dxram) {
		m_dxram = p_dxram;
		m_chunkService = m_dxram.getService(ChunkService.class);
		m_nameserviceService = m_dxram.getService(NameserviceService.class);
		m_lockService = m_dxram.getService(LockService.class);
	}

	/**
	 * Creates a new Chunk
	 * @param p_size
	 *            the size of the Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be created
	 */
	public Chunk createNewChunk(final int p_size) throws DXRAMException {
		Chunk ret = null;

		if (m_chunkService != null) {
			long[] ids = m_chunkService.create(p_size);
			if (ids == null) {
				throw new DXRAMException("Cannot create chunk.");
			}
			
			ret = new Chunk(ids[0], p_size);
		}

		return ret;
	}

	/**
	 * Creates new Chunks
	 * @param p_sizes
	 *            the sizes of the Chunks
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the chunks could not be created
	 */
	public Chunk[] createNewChunks(final int[] p_sizes) throws DXRAMException {
		Chunk[] ret = null;

		if (m_chunkService != null) {
			long[] ids = m_chunkService.create(p_sizes);
			if (ids == null) {
				throw new DXRAMException("Cannot create chunks.");
			}
			
			ret = new Chunk[ids.length];
			for (int i = 0; i < ret.length; i++) {
				ret[i] = new Chunk(ids[i], p_sizes[i]);
			}
		}

		return ret;
	}

	/**
	 * Creates a new Chunk with identifier
	 * @param p_size
	 *            the size of the Chunk
	 * @param p_name
	 *            the identifier of the Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be created
	 */
	public Chunk createNewChunk(final int p_size, final String p_name) throws DXRAMException {
		Chunk ret = null;
		
		ret = createNewChunk(p_size);
		if (ret != null) {
			if (m_nameserviceService != null) {
				m_nameserviceService.register(ret, p_name);
			}
		}

		return ret;
	}

	/**
	 * Creates new Chunks with identifier
	 * @param p_sizes
	 *            the sizes of the Chunks
	 * @param p_name
	 *            the identifier of the first Chunk
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the chunks could not be created
	 */
	public Chunk[] createNewChunks(final int[] p_sizes, final String p_name) throws DXRAMException {
		Chunk[] ret = null;
		
		ret = createNewChunks(p_sizes);
		if (ret != null) {
			if (m_nameserviceService != null) {
				m_nameserviceService.register(ret[0], p_name);
			}
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunk for the given ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public Chunk get(final long p_chunkID) throws DXRAMException {
		Chunk ret = null;

		if (m_chunkService != null) {
			// gets resized automatically, because dynamic sized data structure
			ret = new Chunk(p_chunkID, 0);
			if (m_chunkService.get(ret) != 1) {
				throw new DXRAMException("Getting chunk " + ret + " failed.");
			}
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunks for the given IDs
	 * @param p_chunkIDs
	 *            the IDs of the corresponding Chunks
	 * @return the Chunks for the given IDs
	 * @throws DXRAMException
	 *             if the chunks could not be get
	 */
	public Chunk[] get(final long[] p_chunkIDs) throws DXRAMException {
		Chunk[] ret = null;

		if (m_chunkService != null) {
			
			ret = new Chunk[p_chunkIDs.length];
			for (int i = 0; i < ret.length; i++) {
				// gets resized automatically, because dynamic sized data structure
				ret[i] = new Chunk(p_chunkIDs[i], 0);
			}
			if (m_chunkService.get(ret) != ret.length) {
				throw new DXRAMException("Getting chunks " + ret[0] + "... failed.");
			}
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunk for the given identifier
	 * @param p_name
	 *            the identifier of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public Chunk get(final String p_name) throws DXRAMException {
		Chunk ret = null;
		
		if (m_nameserviceService != null) {
			long chunkID = m_nameserviceService.getChunkID(p_name);
			if (chunkID != ChunkID.INVALID_ID) {
				return get(chunkID);
			}
		}

		return ret;
	}

	/**
	 * Gets the corresponding ChunkID for the given identifier
	 * @param p_name
	 *            the identifier of the corresponding Chunk
	 * @return the ChunkID for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public long getChunkID(final String p_name) throws DXRAMException {
		long ret = -1;

		if (m_nameserviceService != null) {
			ret = m_nameserviceService.getChunkID(p_name);
		}

		return ret;
	}

	/**
	 * Updates the given Chunk
	 * @param p_chunk
	 *            the Chunk to be updated
	 * @throws DXRAMException
	 *             if the chunk could not be put
	 */
	public void put(final Chunk p_chunk) throws DXRAMException {
		put(p_chunk, false);
	}

	/**
	 * Updates the given Chunk
	 * @param p_chunk
	 *            the Chunk to be updated
	 * @param p_releaseLock
	 *            if true a possible lock is released
	 * @throws DXRAMException
	 *             if the chunk could not be put
	 */
	public void put(final Chunk p_chunk, final boolean p_releaseLock) throws DXRAMException {

		if (m_chunkService != null) {
			ChunkLockOperation lockOp;
			if (p_releaseLock) {
				lockOp = ChunkLockOperation.WRITE_LOCK;
			} else {
				lockOp = ChunkLockOperation.NO_LOCK_OPERATION;
			}
			
			if (m_chunkService.put(lockOp, p_chunk) != 1) {
				throw new DXRAMException("Putting chunk " + p_chunk + " failed.");
			}
		}
	}

	/**
	 * Updates given Chunks
	 * @param p_chunks
	 *            the Chunks to be updated
	 * @throws DXRAMException
	 *             if the chunks could not be put
	 */
	public void put(final Chunk[] p_chunks) throws DXRAMException {
		if (m_chunkService != null) {			
			if (m_chunkService.put(p_chunks) != p_chunks.length) {
				throw new DXRAMException("Putting chunks " + p_chunks[0] + "... failed.");
			}
		}
	}

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be locked
	 */
	public Chunk lock(final long p_chunkID) throws DXRAMException {
		return lock(p_chunkID, false);
	}

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @param p_readLock
	 *            true if the lock is a read lock, false otherwise
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be locked
	 */
	public Chunk lock(final long p_chunkID, final boolean p_readLock) throws DXRAMException {
		Chunk ret = null;

		if (m_chunkService != null) {			
			if (m_lockService != null) {
				// gets resized automatically, because dynamic sized data structure
				ret = new Chunk(p_chunkID, 0);
				
				if (m_lockService.lock(!p_readLock, LockService.MS_TIMEOUT_UNLIMITED, ret) != LockService.ErrorCode.SUCCESS) {
					throw new DXRAMException("Locking chunk " + p_chunkID + " failed.");
				}
				
				if (m_chunkService.get(ret) != 1) {
					throw new DXRAMException("Getting chunk " + p_chunkID + " after locking failed.");
				}
			}
		}

		return ret;
	}

	/**
	 * Unlocks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be unlocked
	 */
	public void unlock(final long p_chunkID) throws DXRAMException {
		if (m_lockService != null) {
			m_lockService.unlock(true, p_chunkID);
		}
	}

	/**
	 * Removes the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be removed
	 */
	public void remove(final long p_chunkID) throws DXRAMException {
		if (m_chunkService != null) {
			if (m_chunkService.remove(p_chunkID) != 1) {
				throw new DXRAMException("Removing chunkID " + Long.toHexString(p_chunkID) + " failed.");
			}
		}
	}

	/**
	 * Removes the corresponding Chunk for the giving ID
	 * @param p_chunkIDs
	 *            the IDs of the corresponding Chunks
	 * @throws DXRAMException
	 *             if the chunks could not be removed
	 */
	public void remove(final long[] p_chunkIDs) throws DXRAMException {
		if (m_chunkService != null) {
			if (m_chunkService.remove(p_chunkIDs) != p_chunkIDs.length) {
				throw new DXRAMException("Removing chunkID " + Long.toHexString(p_chunkIDs[0]) + "... failed.");
			}
		}
	}

}
