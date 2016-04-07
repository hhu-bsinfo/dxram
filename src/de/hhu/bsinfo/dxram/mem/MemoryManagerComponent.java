
package de.hhu.bsinfo.dxram.mem;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageJNINativeMemory;

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
public final class MemoryManagerComponent extends AbstractDXRAMComponent {

	private SmallObjectHeap m_rawMemory;
	private CIDTable m_cidTable;
	private ReentrantReadWriteLock m_lock;
	private long m_numActiveChunks;

	private AbstractBootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private StatisticsComponent m_statistics = null;

	private MemoryStatisticsRecorderIDs m_statisticsRecorderIDs = null;

	/**
	 * Error codes to be returned by some methods.
	 */
	public enum MemoryErrorCodes
	{
		SUCCESS,
		UNKNOWN,
		DOES_NOT_EXIST,
		READ,
		WRITE,
		OUT_OF_MEMORY,
	}

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public MemoryManagerComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(MemoryManagerConfigurationValues.Component.RAM_SIZE);
		p_settings.setDefaultValue(MemoryManagerConfigurationValues.Component.SEGMENT_SIZE);
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings)
	{
		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);
		m_statistics = getDependentComponent(StatisticsComponent.class);

		if (m_boot.getNodeRole() != NodeRole.SUPERPEER) {
			registerStatisticsOperations();

			final long ramSize = p_settings.getValue(MemoryManagerConfigurationValues.Component.RAM_SIZE);
			m_logger.trace(getClass(), "Allocating native memory (" + (ramSize / 1024 / 1024) + "mb). This may take a while.");
			m_rawMemory = new SmallObjectHeap(new StorageJNINativeMemory());
			m_rawMemory.initialize(ramSize, p_settings.getValue(MemoryManagerConfigurationValues.Component.SEGMENT_SIZE));
			m_cidTable = new CIDTable(m_boot.getNodeID(), m_statistics, m_statisticsRecorderIDs, m_logger);
			m_cidTable.initialize(m_rawMemory);

			m_lock = new ReentrantReadWriteLock(false);

			m_numActiveChunks = 0;
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		if (m_boot.getNodeRole() != NodeRole.SUPERPEER) {
			m_cidTable.disengage();
			m_rawMemory.disengage();

			m_cidTable = null;
			m_rawMemory = null;
			m_lock = null;
		}

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
	 * Get some status information about the memory manager (free, total amount of memory).
	 * @return Status information.
	 */
	public Status getStatus()
	{
		Status status = new Status();

		status.m_freeMemoryBytes = m_rawMemory.getFreeMemory();
		status.m_totalMemoryBytes = m_rawMemory.getTotalMemory();

		return status;
	}

	/**
	 * The chunk ID 0 is reserved for a fixed index structure.
	 * If the index structure is already created this will delete the old
	 * one and allocate a new block of memory with the same id (0).
	 * @param p_size
	 *            Size for the index chunk.
	 * @return On success the chunk id 0, -1 on failure.
	 */
	public long createIndex(final int p_size)
	{
		assert p_size > 0;

		long address = -1;
		long chunkID = -1;

		if (m_cidTable.get(0) != -1) {
			// delete old entry
			address = m_cidTable.delete(0, false);
			m_rawMemory.free(address);
			m_numActiveChunks--;
		}

		address = m_rawMemory.malloc(p_size);
		if (address >= 0) {
			chunkID = ((long) m_boot.getNodeID() << 48) + 0;
			// register new chunk
			m_cidTable.set(chunkID, address);
			m_numActiveChunks++;
		} else {
			chunkID = -1;
		}

		return chunkID;
	}

	/**
	 * Create a chunk with a specific chunk id (used for migration/recovery).
	 * @param p_chunkId
	 *            Chunk id to assign to the chunk.
	 * @param p_size
	 *            Size of the chunk.
	 * @return The chunk id if successful, -1 if another chunk with the same id already exists
	 *         or allocation memory failed.
	 */
	public long create(final long p_chunkId, final int p_size)
	{
		assert p_size > 0;

		long address = -1;
		long chunkID = -1;

		if (m_cidTable.get(p_chunkId) == -1) {
			address = m_rawMemory.malloc(p_size);
			if (address >= 0) {
				// register new chunk
				m_cidTable.set(p_chunkId, address);
				m_numActiveChunks++;
				chunkID = p_chunkId;
			} else {
				chunkID = -1;
			}
		}

		return chunkID;
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

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create);

		// get new LID from CIDTable
		lid = m_cidTable.getFreeLID();
		if (lid == -1) {
			chunkID = -1;
		} else {
			// first, try to allocate. maybe early return
			m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc, p_size);
			address = m_rawMemory.malloc(p_size);
			m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc);
			if (address >= 0) {
				chunkID = ((long) m_boot.getNodeID() << 48) + lid;
				// register new chunk
				m_cidTable.set(chunkID, address);
				m_numActiveChunks++;
			} else {
				// most likely out of memory
				m_logger.error(getClass(), "Creating chunk with size " + p_size + " failed, most likely out of memory, free " +
						m_rawMemory.getFreeMemory() + ", total " + m_rawMemory.getTotalMemory());

				// put lid back
				m_cidTable.putChunkIDForReuse(lid);
			}
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create);

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
	 * @param p_dataStructure
	 *            Data structure to write the data of its specified ID to.
	 * @return True if getting the chunk payload was successful, false if no chunk with the ID specified by the data
	 *         structure exists.
	 */
	public MemoryErrorCodes get(final DataStructure p_dataStructure)
	{
		long address;
		MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get);

		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			int chunkSize = m_rawMemory.getSizeBlock(address);
			SmallObjectHeapDataStructureImExporter importer = new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
			if (importer.importObject(p_dataStructure) < 0) {
				ret = MemoryErrorCodes.READ;
			}
		}
		else
		{
			ret = MemoryErrorCodes.DOES_NOT_EXIST;
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get);

		return ret;
	}

	/**
	 * Get a chunk when size is unknown.
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            Data structure to write the data of its specified ID to.
	 * @return A byte array with payload if getting the chunk payload was successful, null if no chunk with the ID exists.
	 */
	public byte[] get(final long p_chunkID)
	{
		byte[] ret = null;
		long address;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get);

		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			int chunkSize = m_rawMemory.getSizeBlock(address);
			ret = new byte[chunkSize];

			SmallObjectHeapDataStructureImExporter importer = new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
			if (importer.readBytes(ret) != chunkSize) {
				ret = null;
			}
		} else {
			m_logger.warn(getClass(), "Could not find data for ID=" + p_chunkID);
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get);

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
	 */
	public MemoryErrorCodes put(final DataStructure p_dataStructure)
	{
		long address;
		MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_put);

		address = m_cidTable.get(p_dataStructure.getID());
		if (address > 0) {
			int chunkSize = m_rawMemory.getSizeBlock(address);
			SmallObjectHeapDataStructureImExporter exporter = new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
			if (exporter.exportObject(p_dataStructure) < 0) {
				ret = MemoryErrorCodes.WRITE;
			}
		} else {
			ret = MemoryErrorCodes.DOES_NOT_EXIST;
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_put);

		return ret;
	}

	/**
	 * Removes a Chunk from the memory
	 * This is a management call and has to be locked using lockManage().
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 */
	public MemoryErrorCodes remove(final long p_chunkID) {
		long addressDeletedChunk;
		int size;
		MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remove);

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

			size = m_rawMemory.getSizeBlock(addressDeletedChunk);
			m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_free, size);
			m_rawMemory.free(addressDeletedChunk);
			m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_free);
			m_numActiveChunks--;
		} else {
			ret = MemoryErrorCodes.DOES_NOT_EXIST;
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remove);

		return ret;
	}

	/**
	 * Get the number of currently active chunks/allocated blocks.
	 * @return Number of currently active chunks.
	 */
	public long getNumberOfActiveChunks() {
		return m_numActiveChunks;
	}

	/**
	 * Returns whether this Chunk is stored locally or not.
	 * Only the LID is evaluated and checked. The NID is masked out.
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk is stored locally or not
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
		return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
	}

	/**
	 * Removes the ChunkID of a deleted Chunk that was migrated
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public void prepareChunkIDForReuse(final long p_chunkID) {
		m_cidTable.putChunkIDForReuse(p_chunkID);
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 */
	public ArrayList<Long> getCIDOfAllMigratedChunks() {
		return m_cidTable.getCIDOfAllMigratedChunks();
	}

	/**
	 * Returns the ChunkID ranges of all locally stored Chunks
	 * @return the ChunkID ranges in an ArrayList
	 */
	public ArrayList<Long> getCIDRangesOfAllLocalChunks() {
		return m_cidTable.getCIDRangesOfAllLocalChunks();
	}

	/**
	 * Object containing status information about the memory.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 */
	public static class Status
	{
		private long m_totalMemoryBytes = -1;
		private long m_freeMemoryBytes = -1;

		/**
		 * Constructor
		 */
		public Status()
		{

		}

		/**
		 * Get the total amount of memory in bytes.
		 * @return Total amount of memory in bytes.
		 */
		public long getTotalMemory()
		{
			return m_totalMemoryBytes;
		}

		/**
		 * Get the total amount of free memory in bytes.
		 * @return Amount of free memory in bytes.
		 */
		public long getFreeMemory()
		{
			return m_freeMemoryBytes;
		}
	}

	/**
	 * Register statistics operations for this component.
	 */
	private void registerStatisticsOperations()
	{
		m_statisticsRecorderIDs = new MemoryStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());

		m_statisticsRecorderIDs.m_operations.m_createNIDTable =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_CREATE_NID_TABLE);
		m_statisticsRecorderIDs.m_operations.m_createLIDTable =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_CREATE_LID_TABLE);
		m_statisticsRecorderIDs.m_operations.m_malloc =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_MALLOC);
		m_statisticsRecorderIDs.m_operations.m_free =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_FREE);
		m_statisticsRecorderIDs.m_operations.m_get = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_GET);
		m_statisticsRecorderIDs.m_operations.m_put = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_PUT);
		m_statisticsRecorderIDs.m_operations.m_create =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_CREATE);
		m_statisticsRecorderIDs.m_operations.m_remove =
				m_statistics.createOperation(m_statisticsRecorderIDs.m_id, MemoryStatisticsRecorderIDs.Operations.MS_REMOVE);
	}
}
