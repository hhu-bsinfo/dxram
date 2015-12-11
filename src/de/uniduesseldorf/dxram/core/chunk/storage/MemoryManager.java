
package de.uniduesseldorf.dxram.core.chunk.storage;

import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.LIDElement;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.StatisticsManager;
import de.uniduesseldorf.dxram.utils.locks.JNIReadWriteSpinLock;

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
		m_cidTable = new CIDTable(m_nodeID);
		m_cidTable.initialize(m_rawMemory);

		m_lock = new JNIReadWriteSpinLock();

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
		int chunkSize = -1;
		LIDElement lid = null;

		// get new LID from CIDTable
		lid = m_cidTable.getFreeLID();

		assert lid.getVersion() >= 0;

		chunkSize = p_size + getSizeVersion(lid.getVersion());

		// first, try to allocate. maybe early return
		address = m_rawMemory.malloc(chunkSize);
		if (address >= 0) {
			// register new chunk
			chunkID = ((long) m_nodeID << 48) + lid.getLocalID();
			m_cidTable.set(chunkID, address);

			writeVersion(address, lid.getVersion());
		} else {
			// put lid back
			// TODO is that ok? that's a protected call...
			// check CID table impl
			m_cidTable.putChunkIDForReuse(lid.getLocalID());
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
			size = m_rawMemory.getSizeBlock(address) - getSizeVersion(readVersion(address));
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
			int sizeVersion;

			sizeVersion = getSizeVersion(readVersion(address));
			bytesRead = m_rawMemory.readBytes(address, sizeVersion, p_buffer, p_offset, p_length);
		}

		return bytesRead;
	}

	/**
	 * Get the version of a chunk.
	 * This is an access call and has to be locked using lockAccess().
	 * @param p_chunkID
	 *            ChunkID of the chunk.
	 * @return -1 if chunkID was invalid, otherwise version of the chunk.
	 * @throws MemoryException
	 *             If reading version failed.
	 */
	public int getVersion(final long p_chunkID) throws MemoryException {
		long address = 0;
		int version = -1;

		address = m_cidTable.get(p_chunkID);
		if (address > 0) {
			version = readVersion(address);
		}

		return version;
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
			int curVersion = -1;
			int sizeVersion = -1;
			int sizeVersionNext = -1;

			curVersion = readVersion(address);
			sizeVersion = getSizeVersion(curVersion);
			sizeVersionNext = getSizeVersion(curVersion + 1);

			// check if we have to reallocate due to version growth
			if (sizeVersion != sizeVersionNext) {
				int newTotalSizePayload;
				long newAddress = -1;

				// skip version and read payload
				newTotalSizePayload = m_rawMemory.getSizeBlock(address) - sizeVersion;
				newTotalSizePayload += sizeVersionNext;
				sizeVersion = sizeVersionNext;

				// try to allocate first
				newAddress = m_rawMemory.malloc(newTotalSizePayload);
				if (newAddress != -1) {
					m_rawMemory.free(address);
					m_cidTable.set(p_chunkID, newAddress);
					address = newAddress;
				}
			}

			writeVersion(address, curVersion + 1);
			bytesWritten = m_rawMemory.writeBytes(address, sizeVersion, p_buffer, p_offset, p_length);
		}

		return bytesWritten;
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
		int version;
		long addressDeletedChunk;

		// Get and delete the address from the CIDTable, mark as zombie first
		addressDeletedChunk = m_cidTable.delete(p_chunkID, true);

		// read version
		version = readVersion(addressDeletedChunk);

		// more space for another zombie for reuse in LID store?
		if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
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

	// ----------------------------------------------------------------------------------------

	/**
	 * Write the version of a chunk to the specified address.
	 * Make sure the block at the address is big enough to also fit the address
	 * at the beginning of the chunk's payload!
	 * @param p_address
	 *            Address to write the version to.
	 * @param p_version
	 *            Version number to write.
	 * @throws MemoryException
	 *             If writing the version failed.
	 */
	private void writeVersion(final long p_address, final int p_version) throws MemoryException {
		int versionSize = -1;
		int versionToWrite = -1;

		versionToWrite = p_version;
		versionSize = getSizeVersion(p_version);
		// overflow, reset version
		if (versionSize == -1) {
			versionToWrite = 0;
			versionSize = 0;
		}

		switch (versionSize) {
		case 0:
			m_rawMemory.setCustomState(p_address, versionToWrite);
			break;
		case 1:
			byte b0;

			b0 = (byte) (versionToWrite & 0x7F);

			m_rawMemory.setCustomState(p_address, 2);
			m_rawMemory.writeByte(p_address, 0, b0);
			break;
		case 2:
			byte b1;

			b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
			b1 = (byte) ((versionToWrite >> 7) & 0x7F);

			m_rawMemory.setCustomState(p_address, 2);

			m_rawMemory.writeByte(p_address, 0, b0);
			m_rawMemory.writeByte(p_address, 1, b1);
			break;
		case 3:
			byte b2;

			b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
			b1 = (byte) (((versionToWrite >> 7) & 0x7F) | 0x80);
			b2 = (byte) (((versionToWrite >> 14) & 0x7F));

			m_rawMemory.setCustomState(p_address, 2);

			m_rawMemory.writeByte(p_address, 0, b0);
			m_rawMemory.writeByte(p_address, 1, b1);
			m_rawMemory.writeByte(p_address, 2, b2);
			break;
		case 4:
			byte b3;

			b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
			b1 = (byte) (((versionToWrite >> 7) & 0x7F) | 0x80);
			b2 = (byte) (((versionToWrite >> 14) & 0x7F) | 0x80);
			b3 = (byte) (((versionToWrite >> 21) & 0x7F));

			m_rawMemory.setCustomState(p_address, 2);

			m_rawMemory.writeByte(p_address, 0, b0);
			m_rawMemory.writeByte(p_address, 1, b1);
			m_rawMemory.writeByte(p_address, 2, b2);
			m_rawMemory.writeByte(p_address, 3, b3);
			break;
		default:
			assert 1 == 2;
			break;
		}
	}

	/**
	 * Read the version from the specified address.
	 * Make sure the address points to the start of a chunk
	 * i.e. a valid position that has an address.
	 * @param p_address
	 *            Address to read version from.
	 * @return Version read or -1 if reading version failed or invalid version.
	 * @throws MemoryException
	 *             If reading version from memory failed.
	 */
	private int readVersion(final long p_address) throws MemoryException {
		int version = -1;
		int customState = -1;

		customState = m_rawMemory.getCustomState(p_address);

		switch (customState) {
		case 0:
			version = 0;
			break;
		case 1:
			version = 1;
			break;
		case 2:
			byte b0 = 0;
			byte b1 = 0;
			byte b2 = 0;
			byte b3 = 0;

			b0 = m_rawMemory.readByte(p_address, 0);
			if ((b0 & 0x80) == 1) {
				b1 = m_rawMemory.readByte(p_address, 1);
				if ((b1 & 0x80) == 1) {
					b2 = m_rawMemory.readByte(p_address, 2);
					if ((b2 & 0x80) == 1) {
						b3 = m_rawMemory.readByte(p_address, 3);
					}
				}
			}

			version = ((b3 & 0x7F) << 21)
					| ((b2 & 0x7F) << 14)
					| ((b1 & 0x7F) << 7)
					| (b0 & 0x7F);
			break;
		default:
			assert 1 == 2;
			break;
		}

		return version;
	}

	/**
	 * Get the amount of bytes required to store the spcified version.
	 * @param p_version
	 *            Version to store.
	 * @return Number of bytes required.
	 */
	private int getSizeVersion(final int p_version) {
		assert p_version >= 0;

		int versionSize = -1;

		// max supported 2^28
		// versions 0 and 1 are stored using custom
		// state
		if (p_version <= 1) {
			versionSize = 0;
			// 2^7
		} else if (p_version <= 0x7F) {
			versionSize = 1;
			// 2^14
		} else if (p_version <= 0x4000) {
			versionSize = 2;
			// 2^21
		} else if (p_version <= 0x200000) {
			versionSize = 3;
			// 2^28
		} else if (p_version <= 10000000) {
			versionSize = 4;
		}

		return versionSize;
	}
}
