
package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;
import de.hhu.bsinfo.utils.StatisticsManager;
import de.hhu.bsinfo.utils.locks.JNIReadWriteSpinLock;

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
	
	// Attributes
	private boolean m_enableMemoryStatistics;
	private SmallObjectHeap m_rawMemory;
	private CIDTable m_cidTable;
	private JNIReadWriteSpinLock m_lock;

	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 * @param p_nodeID
	 *            ID of the node this manager is running on.
	 */
	public MemoryManagerComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(MemoryManagerConfigurationValues.Component.RAM_SIZE);
		p_settings.setDefaultValue(MemoryManagerConfigurationValues.Component.SEGMENT_SIZE);
		p_settings.setDefaultValue(MemoryManagerConfigurationValues.Component.STATISTICS);
	}
	
	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) 
	{
		m_boot = getDependentComponent(BootComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);
		
		m_enableMemoryStatistics = p_settings.getValue(MemoryManagerConfigurationValues.Component.STATISTICS);

		if (m_enableMemoryStatistics) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}

		m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory());
		m_rawMemory.initialize(
				p_settings.getValue(MemoryManagerConfigurationValues.Component.RAM_SIZE),
				p_settings.getValue(MemoryManagerConfigurationValues.Component.SEGMENT_SIZE));
		m_cidTable = new CIDTable(m_boot.getNodeID(), m_enableMemoryStatistics, m_logger);
		m_cidTable.initialize(m_rawMemory);

		m_lock = new JNIReadWriteSpinLock();
		
		MemoryStatistic.getInstance().initMemory(p_settings.getValue(MemoryManagerConfigurationValues.Component.RAM_SIZE));
		
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
	 * @return Address of the allocated chunk or -1 if creating the chunk failed.
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
				m_cidTable.set(lid, address);
				chunkID = ((long) m_boot.getNodeID() << 48) + lid;
				
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
	 *            ChunkID of the chunk, the local id gets extracted, the node ID ignored.
	 * @return Size of the chunk or -1 if the chunkID was invalid.
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
	 * @param p_dataStructure Data structure to write the data of its specified ID to.
	 * @return True if getting the chunk payload was successful, false if no chunk with the ID specified by the data structure exists.l
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
			
			ret = true;
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
		address = m_cidTable.get(p_chunkID & 0xFFFFFFFFFFFFL);

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
		return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
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
