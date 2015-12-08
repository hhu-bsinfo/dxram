
package de.uniduesseldorf.dxram.core.chunk.storage;

import de.uniduesseldorf.dxram.core.chunk.DataStructure;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.StatisticsManager;
import de.uniduesseldorf.dxram.utils.locks.JNIReadWriteSpinLock;

import de.uniduesseldorf.soh.SmallObjectHeap;
import de.uniduesseldorf.soh.StorageUnsafeMemory;

/**
 * Interface to access the local heap. Features for migration
 * and other tasks are provided as well.
 * Using this class, you have to take care of locking certain calls.
 * This depends on the type (access or manage). Check the documentation
 * of each call to figure how to handle them. Make use of this by combining
 * multiple calls within a single critical section to avoid locking overhead.
 * @author Florian Klein
 *         13.02.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class MemoryManager {

	// Attributes
	private long m_nodeID;
	private SmallObjectHeap m_rawMemory;
	private SmallObjectHeapDSReaderWriter m_smallObjectHeapDSReaderWriter;
	private CIDTable m_cidTable;
	private JNIReadWriteSpinLock m_lock;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 * @param p_nodeID
	 *            ID of the node this manager is running on.
	 */
	public MemoryManager(final long p_nodeID) {
		m_nodeID = p_nodeID;
	}

	// Methods
	/**
	 * Initializes the SmallObjectHeap and the CIDTable
	 * @param p_size
	 *            the size of the memory
	 * @param p_segmentSize
	 *            size of a single segment
	 * @param p_registerStatistics
	 *            True to register memory statistics, false otherwise.
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be initialized
	 */
	public long initialize(final long p_size, final long p_segmentSize, final boolean p_registerStatistics)
			throws MemoryException {
		long ret;

		if (p_registerStatistics) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}

		m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory());
		ret = m_rawMemory.initialize(p_size, p_segmentSize);
		m_smallObjectHeapDSReaderWriter = new SmallObjectHeapDSReaderWriter(m_rawMemory);
		m_cidTable = new CIDTable(m_nodeID);
		m_cidTable.initialize(m_rawMemory);

		m_lock = new JNIReadWriteSpinLock();
		
		MemoryStatistic.getInstance().initMemory(p_size);

		return ret;
	}

	/**
	 * Disengages the RawMemory and the CIDTable
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be disengaged
	 */
	public void disengage() throws MemoryException {
		m_cidTable.disengage();
		m_rawMemory.disengage();

		m_cidTable = null;
		m_smallObjectHeapDSReaderWriter = null;
		m_rawMemory = null;
		m_lock = null;
	}

	/**
	 * Lock the memory for a management task (create, put, remove).
	 */
	public void lockManage() {
		m_lock.writeLock().lock();
	}

	/**
	 * Lock the memory for an access task (get).
	 */
	public void lockAccess() {
		m_lock.readLock().lock();
	}

	/**
	 * Unlock the memory after a management task (create, put, remove).
	 */
	public void unlockManage() {
		m_lock.writeLock().unlock();
	}

	/**
	 * Unlock the memory after an access task (get).
	 */
	public void unlockAccess() {
		m_lock.readLock().unlock();
	}

	/**
	 * Create a new chunk.
	 * This is a management call and has to be locked using lockManage().
	 * @param p_size
	 *            Size in bytes of the payload the chunk contains.
	 * @return Address of the allocated chunk.
	 * @throws MemoryException
	 *             If allocation failed.
	 */
	public long create(final int p_size) throws MemoryException {
		assert p_size > 0;

		long address = -1;
		long chunkID = -1;
		long lid = -1;
		
		// get new LID from CIDTable
		lid = m_cidTable.getFreeLID();
		if (lid == -1)
		{
			chunkID = -1;
		}
		else
		{
			// first, try to allocate. maybe early return
			address = m_rawMemory.malloc(p_size);
			if (address >= 0) {
				// register new chunk
				chunkID = ((long) m_nodeID << 48) + lid;
				m_cidTable.set(chunkID, address);
				
				// TODO switch to turn on/off
				// TODO have free call for free
				MemoryStatistic.getInstance().malloc(blockSize);
			} else {
				// put lid back
				m_cidTable.putChunkIDForReuse(lid);
			}
		}

		return chunkID;
	}

	/**
	 * Check if a specific chunkID exists.
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            ChunkID to check.
	 * @return True if a chunk with that ID exists, false otherwise.
	 * @throws MemoryException
	 *             If reading from CIDTable fails.
	 */
	public boolean exists(final long p_chunkID) throws MemoryException {
		long address = -1;

		address = m_cidTable.get(p_chunkID);
		return address != 0;
	}

	/**
	 * Get the size of a chunk (payload only, i.e. minus size for version).
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            ChunkID of the chunk.
	 * @return Size of the chunk or -1 if the chunkID was invalid.
	 * @throws MemoryException
	 *             If getting the size of the chunk failed.
	 */
	public int getSize(final long p_chunkID) throws MemoryException {
		long address = -1;
		int size = -1;

		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			size = m_rawMemory.getSizeBlock(address);
		}

		return size;
	}

	/**
	 * Get the payload of a chunk.
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            ChunkID of the chunk.
	 * @param p_buffer
	 *            Buffer to copy the payload to.
	 * @param p_offset
	 *            Start offset within the buffer.
	 * @param p_length
	 *            Number of bytes to get.
	 * @return Number of bytes written to buffer.
	 * @throws MemoryException
	 *             If reading chunk data failed.
	 */
	public int get(final long p_chunkID, final byte[] p_buffer, final int p_offset, final int p_length) throws MemoryException {
		long address;
		int bytesRead = -1;

		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			bytesRead = m_rawMemory.readBytes(address, 0, p_buffer, p_offset, p_length);
		}

		return bytesRead;
	}
	
	public boolean get(final DataStructure p_dataStructure) throws MemoryException
	{
		long address;
		boolean ret = false;
		
		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			p_dataStructure.read(address, m_smallObjectHeapDSReaderWriter);
			ret = true;
		}
		
		return ret;
	}

	/**
	 * Put some data into a chunk.
	 * This is a management call and has to be locked using lockManage().
	 * @param p_chunkID
	 *            ChunkID of the chunk.
	 * @param p_buffer
	 *            Buffer with data to put.
	 * @param p_offset
	 *            Start offset within the buffer.
	 * @param p_length
	 *            Number of bytes to put.
	 * @return Number of bytes put/written to the chunk.
	 * @throws MemoryException
	 *             If writing data failed.
	 */
	public int put(final long p_chunkID, final byte[] p_buffer, final int p_offset, final int p_length) throws MemoryException {
		long address;
		int bytesWritten = -1;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			bytesWritten = m_rawMemory.writeBytes(address, 0, p_buffer, p_offset, p_length);
		}

		return bytesWritten;
	}
	
	public boolean put(final DataStructure p_dataStructure) throws MemoryException
	{
		long address;
		boolean ret = false;
		
		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			p_dataStructure.write(address, m_smallObjectHeapDSReaderWriter);
			ret = true;
		}

		return ret;
	}

	/**
	 * Delete a chunk.
	 * This is a management call and has to be locked using lockManage().
	 * @param p_chunkID
	 *            ID of the chunk.
	 * @return True if deleted, false if there was no chunk if the spcified ID to delete.
	 * @throws MemoryException
	 *             If deleting chunk failed.
	 */
	public boolean delete(final long p_chunkID) throws MemoryException {
		long address;
		boolean deleted = false;

		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			m_rawMemory.free(address);
			deleted = true;
		}

		return deleted;
	}

	// TODO
	// public int resize(final long p_chunkID, final int p_newSize)
	// {
	//
	// }

	/**
	 * Returns whether this Chunk is stored locally or not
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk is stored locally or not
	 * @throws MemoryException
	 *             if the Chunk could not be checked
	 */
	public boolean isResponsible(final long p_chunkID) throws MemoryException {
		long address;

		// Get the address from the CIDTable
		address = m_cidTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		return address > 0;
	}

	/**
	 * Returns whether this Chunk was migrated here or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk was migrated here or not
	 */
	public boolean wasMigrated(final long p_chunkID) {
		return ChunkID.getCreatorID(p_chunkID) != NodeID.getLocalNodeID();
	}

	/**
	 * Removes a Chunk from the memory
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void remove(final long p_chunkID) throws MemoryException {
		long addressDeletedChunk;

		// Get and delete the address from the CIDTable, mark as zombie first
		addressDeletedChunk = m_cidTable.delete(p_chunkID, true);

		// more space for another zombie for reuse in LID store?
		if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
			// detach reference to zombie
			m_cidTable.delete(p_chunkID, false);
		} else {
			// no space for zombie in LID store, keep him "alive" in table
		}
		
		m_rawMemory.free(addressDeletedChunk);
		MemoryStatistic.getInstance().malloc(blockSize);
	}

	/**
	 * Removes the ChunkID of a deleted Chunk that was migrated
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void prepareChunkIDForReuse(final long p_chunkID) throws MemoryException {
		m_cidTable.putChunkIDForReuse(p_chunkID);
	}

	// FIXME take back in
	// /**
	// * Returns the ChunkIDs of all migrated Chunks
	// * @return the ChunkIDs of all migrated Chunks
	// * @throws MemoryException
	// * if the CIDTable could not be completely accessed
	// */
	// public ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
	// return m_cidTable.getCIDOfAllMigratedChunks();
	// }
	//
	// /**
	// * Returns the ChunkID ranges of all locally stored Chunks
	// * @return the ChunkID ranges in an ArrayList
	// * @throws MemoryException
	// * if the CIDTable could not be completely accessed
	// */
	// public ArrayList<Long> getCIDrangesOfAllLocalChunks() throws MemoryException {
	// return m_cidTable.getCIDrangesOfAllLocalChunks();
	// }
}
