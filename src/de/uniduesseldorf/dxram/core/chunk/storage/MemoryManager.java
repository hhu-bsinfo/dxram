
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.mem.RawMemory;
import de.uniduesseldorf.dxram.core.chunk.mem.StorageUnsafeMemory;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.LIDElement;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.StatisticsManager;

/**
 * Controls the access to the RawMemory and the CIDTable
 * @author Florian Klein
 *         13.02.2014
 */
public final class MemoryManager {

	// Attributes
	private AtomicLong m_nextLocalID;

	private RawMemory m_rawMemory;
	private CIDTable m_cidTable;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 */
	public MemoryManager() {}

	// Methods
	/**
	 * Initializes the RawMemory and the CIDTable
	 * @param p_size
	 *            the size of the memory
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be initialized
	 */
	public long initialize(final long p_size) throws MemoryException {
		long ret;
		long segmentSize;

		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_MEMORY)) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}
		
		segmentSize = Core.getConfiguration().getLongValue(ConfigurationConstants.RAM_SEGMENT_SIZE);

		m_nextLocalID = new AtomicLong(1);

		m_rawMemory = new RawMemory(new StorageUnsafeMemory());
		ret = m_rawMemory.initialize(p_size, segmentSize);
		m_cidTable = new CIDTable();
		m_cidTable.initialize(m_rawMemory);

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
		m_rawMemory = null;
		m_nextLocalID = null;
	}

	/**
	 * Gets the next free local ID
	 * @return the next free local ID
	 */
	public LIDElement getNextLocalID() {
		LIDElement ret = null;

		// Try to get free ID from the CIDTable
		try {
			ret = m_cidTable.getFreeLID();
		} catch (final DXRAMException e) {}

		// If no free ID exist, get next local ID
		if (ret == null) {
			ret = new LIDElement(m_nextLocalID.getAndIncrement(), 0);
		}

		return ret;
	}

	/**
	 * Gets the next free local IDs
	 * @param p_count
	 *            the number of free local IDs
	 * @return the next free local IDs
	 */
	public LIDElement[] getNextLocalIDs(final int p_count) {
		LIDElement[] ret = null;
		int count = 0;
		long localID;

		ret = new LIDElement[p_count];
		for (int i = 0; i < p_count; i++) {
			try {
				ret[i] = m_cidTable.getFreeLID();
			} catch (final DXRAMException e) {}
			if (ret[i] == null) {
				count++;
			}
		}

		localID = m_nextLocalID.getAndAdd(count);
		count = 0;
		for (int i = 0; i < p_count; i++) {
			if (ret[i] == null) {
				ret[i] = new LIDElement(localID + count++, 0);
			}
		}

		return ret;
	}

	/**
	 * Gets the current local ID
	 * @return the current local ID
	 */
	public long getCurrentLocalID() {
		long ret;

		ret = m_nextLocalID.get() - 1;

		return ret;
	}

	/**
	 * Puts a Chunk in the memory
	 * @param p_chunk
	 *            the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be put
	 */
	public void put(final Chunk p_chunk) throws MemoryException {
		int version;
		long chunkID;
		long address;
		byte sizeVersion;
		int totalChunkSize;

		chunkID = p_chunk.getChunkID();
		version = p_chunk.getVersion();
		sizeVersion = getSizeVersion(version);
		totalChunkSize = p_chunk.getSize() + sizeVersion;

		// Get the address from the CIDTable
		address = m_cidTable.get(chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address <= 0) {
			address = m_rawMemory.malloc(totalChunkSize);
			m_cidTable.set(chunkID, address);
		} else {
			// check if we have to expand
			long oldSize;

			oldSize = m_rawMemory.getSizeMemoryBlock(address);
			if (oldSize < totalChunkSize) {
				// re-allocate
				m_rawMemory.free(address);
				address = m_rawMemory.malloc(totalChunkSize);
				m_cidTable.set(chunkID, address);
			}
		}

		// set the size of the version header
		m_rawMemory.setCustomState(address, sizeVersion - 1);
		writeVersion(address, version, sizeVersion);
		m_rawMemory.writeBytes(address, sizeVersion, p_chunk.getData().array());
	}

	/**
	 * Gets the Chunk for the given ChunkID from the memory
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the corresponding Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public Chunk get(final long p_chunkID) throws MemoryException {
		Chunk ret = null;
		long address;

		// Get the address from the CIDTable
		address = m_cidTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address > 0) {
			int version;
			int sizeVersion;
			byte[] data;

			sizeVersion = m_rawMemory.getCustomState(address) + 1;
			version = readVersion(address, sizeVersion);

			// make sure to skip version data
			data = m_rawMemory.readBytes(address, sizeVersion);

			ret = new Chunk(p_chunkID, data, version);
		}

		return ret;
	}

//	public Chunk resize(final long p_chunkID, final long p_newSize) throws MemoryException {
//		Chunk ret = null;
//		long address = -1;
//
//		// Get the address from the CIDTable
//		address = m_cidTable.get(p_chunkID);
//
//		// If address <= 0, the Chunk does not exists in the memory
//		if (address > 0) {
//			long newAddress = -1;
//			
//			// try reallocating the block
//			newAddress = m_rawMemory.realloc(address, p_newSize);
//			if (newAddress != address)
//			{
//				int customState = -1;
//				byte[] data = null;
//				
//				// copy data and free old block
//				customState = m_rawMemory.getCustomState(address);
//				data = m_rawMemory.readBytes(address);
//				
//				m_rawMemory.setCustomState(newAddress, customState);
//				
//				// if the new block is smaller, trunc data
//				if (p_newSize < data.length)
//					m_rawMemory.writeBytes(newAddress, data, p_);
//				else
//					m_rawMemory.writeBytes(newAddress, data);
//			}
//			
//			
//			int version;
//			int sizeVersion;
//			byte[] data;
//
//			sizeVersion = m_rawMemory.getCustomState(address) + 1;
//			version = readVersion(address, sizeVersion);
//
//			// make sure to skip version data
//			data = m_rawMemory.readBytes(address, sizeVersion);
//
//			ret = new Chunk(p_chunkID, data, version);
//		}
//
//		return ret;
//	}
	
	/**
	 * Returns whether this Chunk is stored locally or not
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
		int version;
		int sizeVersion;
		long addressDeletedChunk;

		// Get and delete the address from the CIDTable, mark as zombie first
		addressDeletedChunk = m_cidTable.delete(p_chunkID, true);

		// read version
		sizeVersion = m_rawMemory.getCustomState(addressDeletedChunk) + 1;
		version = readVersion(addressDeletedChunk, sizeVersion);

		// more space for another zombie for reuse in LID store?
		if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID), version)) {
			// detach reference to zombie and free memory
			m_cidTable.delete(p_chunkID, false);
			m_rawMemory.free(addressDeletedChunk);
		} else {
			// no space for zombie in LID store, keep him "alive" in memory
		}
	}

	/**
	 * Removes the ChunkID of a deleted Chunk that was migrated
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_version
	 *            the version
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void prepareChunkIDForReuse(final long p_chunkID, final int p_version) throws MemoryException {
		m_cidTable.putChunkIDForReuse(p_chunkID, p_version);
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
		return m_cidTable.getCIDOfAllMigratedChunks();
	}

	/**
	 * Returns the ChunkID ranges of all locally stored Chunks
	 * @return the ChunkID ranges in an ArrayList
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public ArrayList<Long> getCIDrangesOfAllLocalChunks() throws MemoryException {
		return m_cidTable.getCIDrangesOfAllLocalChunks();
	}

	/**
	 * Get the necessary storage size for the given version number.
	 * @param p_version
	 *            Version number to get the storage size for.
	 * @return Storage size for the specified version or -1 if size not supported.
	 */
	protected byte getSizeVersion(final int p_version) {
		byte ret;

		Contract.check(p_version >= 0, "Invalid version, < 0");

		// max supported 2^24
		if (p_version <= 0xFF) {
			ret = 1;
		} else if (p_version <= 0xFFFF) {
			ret = 2;
		} else if (p_version <= 0xFFFFFF) {
			ret = 3;
		} else {
			ret = -1;
		}

		return ret;
	}

	/**
	 * Write the version number to the specified position.
	 * @param p_address
	 *            Address to write version number to.
	 * @param p_version
	 *            Version number/value.
	 * @param p_size
	 *            Storage size this number needs.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	protected void writeVersion(final long p_address, final int p_version, final int p_size)
			throws MemoryException {
		switch (p_size) {
		case 1:
			m_rawMemory.writeByte(p_address, (byte) (p_version & 0xFF));
			break;
		case 2:
			m_rawMemory.writeShort(p_address, (short) (p_version & 0xFFFF));
			break;
		case 3:
			// store as big endian
			m_rawMemory.writeByte(p_address, (byte) ((p_version >> 16) & 0xFF));
			m_rawMemory.writeShort(p_address, 2, (short) (p_version & 0xFFFF));
			break;
		default:
			assert 1 == 2;
			break;
		}
	}

	/**
	 * Read the version number from the specified location.
	 * @param p_address
	 *            Address to read the version number from.
	 * @param p_size
	 *            Storage size of the version number to read.
	 * @return Version number read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	protected int readVersion(final long p_address, final int p_size) throws MemoryException {
		int ret;

		switch (p_size) {
		case 1:
			ret = (int) m_rawMemory.readByte(p_address) & 0xFF;
			break;
		case 2:
			ret = (int) m_rawMemory.readShort(p_address) & 0xFF;
			break;
		case 3:
			int tmp;

			tmp = 0;
			tmp |= (m_rawMemory.readByte(p_address) << 16) & 0xFF;
			tmp |= m_rawMemory.readShort(p_address, 2) & 0xFF;
			ret = tmp;
			break;
		default:
			assert 1 == 2;
			ret = -1;
			break;
		}

		return ret;
	}
}
