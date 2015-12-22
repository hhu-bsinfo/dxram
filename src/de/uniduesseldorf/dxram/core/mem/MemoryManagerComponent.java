
package de.uniduesseldorf.dxram.core.mem;

import de.uniduesseldorf.dxram.core.data.DataStructure;
import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.statistics.StatisticsConfigurationValues;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.soh.SmallObjectHeap;
import de.uniduesseldorf.soh.StorageUnsafeMemory;
import de.uniduesseldorf.utils.StatisticsManager;
import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.locks.JNIReadWriteSpinLock;

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
public final class MemoryManagerComponent extends DXRAMComponent {

	public static final String COMPONENT_IDENTIFIER = "MemoryManager";
	
	// Attributes
	private boolean m_enableMemoryStatistics;
	private SmallObjectHeap m_rawMemory;
	private CIDTable m_cidTable;
	private JNIReadWriteSpinLock m_lock;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 * @param p_nodeID
	 *            ID of the node this manager is running on.
	 */
	public MemoryManagerComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);
	}
	
	@Override
	protected void registerConfigurationValuesComponent(final Configuration p_configuration) {
		p_configuration.registerConfigurationEntries(MemoryManagerConfigurationValues.CONFIGURATION_ENTRIES);
	}
	
	@Override
	protected boolean initComponent(final Configuration p_configuration) 
	{
		p_configuration.getLongValue(MemoryManagerConfigurationValues.MEM_SIZE);

		m_enableMemoryStatistics = p_configuration.getBooleanValue(StatisticsConfigurationValues.STATISTIC_MEMORY);

		if (m_enableMemoryStatistics) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}

		m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory());
		m_rawMemory.initialize(
				p_configuration.getLongValue(MemoryManagerConfigurationValues.MEM_SIZE), 
				p_configuration.getLongValue(MemoryManagerConfigurationValues.MEM_SEGMENT_SIZE));
		m_cidTable = new CIDTable(getSystemData().getNodeID(), m_enableMemoryStatistics);
		m_cidTable.initialize(m_rawMemory);

		m_lock = new JNIReadWriteSpinLock();
		
		MemoryStatistic.getInstance().initMemory(p_configuration.getLongValue(MemoryManagerConfigurationValues.MEM_SIZE));
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_cidTable.disengage();
		m_rawMemory.disengage();

		m_cidTable = null;
		m_rawMemory = null;
		m_lock = null;
		
		return true;
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
	public long create(final int p_size) {
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
				chunkID = ((long) getSystemData().getNodeID() << 48) + lid;
				m_cidTable.set(chunkID, address);
				
				if (m_enableMemoryStatistics) {
					MemoryStatistic.getInstance().malloc(p_size);
				}
			} else {
				// put lid back
				m_cidTable.putChunkIDForReuse(lid);
			}
		}

		return chunkID;
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
	public int getSize(final long p_chunkID) {
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
	public boolean get(final DataStructure p_dataStructure)
	{
		long address;
		boolean ret = false;
		
		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			int chunkSize = m_rawMemory.getSizeBlock(address);
			SmallObjectHeapDataStructureImExporter importer = new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
			if (importer.importObject(p_dataStructure) >= 0) {
				ret = true;
			}
		}
		
		return ret;
	}
	
	/**
	 * Put some data into a chunk.
	 * This is an access call and has to be locked using lockAccess().
	 * Note: lockAccess() does NOT take care of data races of the data to write.
	 * The caller has to take care of proper locking to avoid consistency issue with his data.
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
	public boolean put(final DataStructure p_dataStructure)
	{
		long address;
		boolean ret = false;
		
		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			int chunkSize = m_rawMemory.getSizeBlock(address);
			SmallObjectHeapDataStructureImExporter exporter = new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
			if (exporter.exportObject(p_dataStructure) >= 0) {
				ret = true;
			}
		}

		return ret;
	}

	/**
	 * Removes a Chunk from the memory
	 * This is a management call and has to be locked using lockManage().
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public boolean remove(final long p_chunkID) {
		long addressDeletedChunk;
		int size;
		boolean ret = false;

		// Get and delete the address from the CIDTable, mark as zombie first
		addressDeletedChunk = m_cidTable.delete(p_chunkID, true);
		if (addressDeletedChunk != -1)
		{
			// more space for another zombie for reuse in LID store?
			if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
				// detach reference to zombie
				m_cidTable.delete(p_chunkID, false);
			} else {
				// no space for zombie in LID store, keep him "alive" in table
			}
			
			m_rawMemory.free(addressDeletedChunk);
			if (m_enableMemoryStatistics) {
				size = m_rawMemory.getSizeBlock(addressDeletedChunk);
				MemoryStatistic.getInstance().free(size);
			}
		}
		
		return ret;
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
	public boolean exists(final long p_chunkID) {
		long address;

		// Get the address from the CIDTable
		address = m_cidTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in memory
		return address > 0;
	}

	/**
	 * Returns whether this Chunk was migrated here or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk was migrated here or not
	 */
	public boolean dataWasMigrated(final long p_chunkID) {
		return ChunkID.getCreatorID(p_chunkID) != getSystemData().getNodeID();
	}

	/**
	 * Removes the ChunkID of a deleted Chunk that was migrated
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void prepareChunkIDForReuse(final long p_chunkID) {
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
