
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.LIDElement;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Pair;
import de.uniduesseldorf.dxram.utils.StatisticsManager;

/**
 * Controls the access to the RawMemory and the CIDTable
 * @author Florian Klein
 *         13.02.2014
 */
public final class MemoryManager {

	// Attributes
	private static AtomicLong m_nextLocalID;
	
	public static final long INVALID_CHUNK_ID = ChunkID.INVALID_ID;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 */
	private MemoryManager() {}
	
	// -----------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------

	// Methods
	/**
	 * Initializes the RawMemory and the CIDTable
	 * @param p_size
	 *            the size of the memory
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be initialized
	 */
	public static long initialize(final long p_size) throws MemoryException {
		long ret;

		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_MEMORY)) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}

		m_nextLocalID = new AtomicLong(1);

		ret = RawMemory.initialize(p_size);
		CIDTable.initialize();

		return ret;
	}

	/**
	 * Disengages the RawMemory and the CIDTable
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be disengaged
	 */
	public static void disengage() throws MemoryException {
		CIDTable.disengage();
		RawMemory.disengage();

		m_nextLocalID = null;
	}

	/**
	 * Gets the next free local ID
	 * @return the next free local ID
	 */
	public static LIDElement getNextLocalID() {
		LIDElement ret = null;

		// Try to get free ID from the CIDTable
		try {
			ret = CIDTable.getFreeLID();
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
	public static LIDElement[] getNextLocalIDs(final int p_count) {
		LIDElement[] ret = null;
		int count = 0;
		long localID;

		ret = new LIDElement[p_count];
		for (int i = 0; i < p_count; i++) {
			try {
				ret[i] = CIDTable.getFreeLID();
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
	public static long getCurrentLocalID() {
		long ret;

		ret = m_nextLocalID.get() - 1;

		return ret;
	}
	
	public static boolean createChunk(final long p_chunkID, final int p_size) throws MemoryException
	{
		boolean ret;
		long address;
		
		// check first if exists
		address = CIDTable.get(p_chunkID);
		if (address <= 0) {
			ret = false;
		} else {
			// + 1 byte default for version number
			address = RawMemory.malloc(p_size + 1);
			CIDTable.set(p_chunkID, address);
			ret = true;
		}
		
		return ret;
	}

	public static boolean chunkExists(final long p_chunkID) throws MemoryException
	{
		return CIDTable.get(p_chunkID) != 0;
	}
	
	public static int readChunkVersion(final long p_chunkID) throws MemoryException
	{
		long address;
		int version;
		
		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);
		
		if (address <= 0)
		{
			// TODO log?
			version = -1;
		} else {
			version = readVersion(address, RawMemory.getCustomState(address) + 1);
		}
		
		return version;
	}
	
	public static int readChunkPayloadSize(final long p_chunkID) throws MemoryException {
		long address;
		int versionSize;
		int size;

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		if (address <= 0) {
			// TODO log?
			size = -1;
		} else {
			versionSize = RawMemory.getCustomState(address) + 1;
			size = RawMemory.getSize(address) - versionSize;
		}

		return size;
	}
	
	public static int readChunkPayload(final long p_chunkID, final int p_payloadOffset, final int p_length, final int p_bufferOffset, final byte[] p_buffer) throws MemoryException
	{		
		long address;
		int versionSize;
		int read;
		
		// TODO Stefan: contract to check offset + length with buffer? 
		// or have asserts only and runtime checks on api level? <-

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		if (address <= 0) {
			// TODO log?
			read = -1;
		} else {
			// skip version when reading payload
			versionSize = RawMemory.getCustomState(address) + 1;
			read =  RawMemory.readBytes(address, versionSize + p_bufferOffset, p_length, p_bufferOffset, p_buffer);
		}
		
		return read;
	}
	
	public static int writeChunkPayload(final long p_chunkID, final int p_payloadOffset, final int p_length, final int p_bufferOffset, final byte[] p_buffer) throws MemoryException {
		long address;
		int written;
		int currentVersionSize;
		int currentVersion;
		int currentVersionPayloadSize;
		int currentVersionTotalChunkSize;
		int nextVersionSize;
		int nextVersion;
		int nextVersionTotalChunkSize;
		
		// TODO Stefan: contract to check offset + length with buffer? 
		// or have asserts only and runtime checks on api level? <-

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		if (address <= 0)
		{
			// TODO log?
			written = -1;
		}
		else
		{
			// get all metadata about the chunk
			currentVersionSize = RawMemory.getCustomState(address) + 1;
			currentVersion = readVersion(address, currentVersionSize);
			currentVersionTotalChunkSize = RawMemory.getSize(address);
			currentVersionPayloadSize = currentVersionTotalChunkSize - currentVersionSize;
			
			// check if we have to expand the chunk due to version size growth
			nextVersion = currentVersion + 1;
			nextVersionSize = getSizeVersion(nextVersion);
			nextVersionTotalChunkSize = currentVersionPayloadSize + nextVersionSize;
	
			if (currentVersionTotalChunkSize < nextVersionTotalChunkSize) {
				// re-allocate
				RawMemory.free(address);
				address = RawMemory.malloc(nextVersionTotalChunkSize);
				CIDTable.set(p_chunkID, address);
			}
	
			// write data
			if (currentVersionSize != nextVersionSize)
				RawMemory.setCustomState(address, nextVersionSize - 1);
			writeVersion(address, nextVersion, nextVersionSize);
			written = RawMemory.writeBytes(address, nextVersionSize + p_bufferOffset, p_length, p_bufferOffset, p_buffer);
		}
		
		return written;
	}

	/**
	 * Returns whether this Chunk is stored locally or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk is stored locally or not
	 * @throws MemoryException
	 *             if the Chunk could not be checked
	 */
	public static boolean isResponsibleForChunk(final long p_chunkID) throws MemoryException {
		long address;

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		return address > 0;
	}

	/**
	 * Returns whether this Chunk was migrated here or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk was migrated here or not
	 */
	public static boolean wasChunkMigrated(final long p_chunkID) {
		return ChunkID.getCreatorID(p_chunkID) != NodeID.getLocalNodeID();
	}

	/**
	 * Removes a Chunk from the memory
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public static void removeChunk(final long p_chunkID) throws MemoryException {
		Pair<Boolean, Long> chunkCleanup;

		// Get and delete the address from the CIDTable
		chunkCleanup = CIDTable.delete(p_chunkID);

		// check if we have to cleanup the zombie chunk
		// the table can tell us not to do this to keep the
		// zombie "alive" in memory for later garbage collection.
		// The table marks it as deleted, but keeps the address
		// for this purpose
		if (chunkCleanup.first()) {
			RawMemory.free(chunkCleanup.second());
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
	public static void prepareChunkIDForReuse(final long p_chunkID, final int p_version) throws MemoryException {
		CIDTable.putChunkIDForReuse(p_chunkID, p_version);
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public static ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
		return CIDTable.getCIDOfAllMigratedChunks();
	}
	
	// -----------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------

	/**
	 * Get the necessary storage size for the given version number.
	 * @param p_version
	 *            Version number to get the storage size for.
	 * @return Storage size for the specified version or -1 if size not supported.
	 */
	private static byte getSizeVersion(final int p_version) {
		assert p_version >= 0;
		
		byte ret;

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
	private static void writeVersion(final long p_address, final int p_version, final int p_size)
			throws MemoryException {
		switch (p_size) {
		case 1:
			RawMemory.writeByte(p_address, (byte) (p_version & 0xFF));
			break;
		case 2:
			RawMemory.writeShort(p_address, (short) (p_version & 0xFFFF));
			break;
		case 3:
			// store as big endian
			RawMemory.writeByte(p_address, (byte) ((p_version >> 16) & 0xFF));
			RawMemory.writeShort(p_address, 2, (short) (p_version & 0xFFFF));
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
	private static int readVersion(final long p_address, final int p_size) throws MemoryException {
		int ret;

		switch (p_size) {
		case 1:
			ret = (int) RawMemory.readByte(p_address) & 0xFF;
			break;
		case 2:
			ret = (int) RawMemory.readShort(p_address) & 0xFF;
			break;
		case 3:
			int tmp;

			tmp = 0;
			tmp |= (RawMemory.readByte(p_address) << 16) & 0xFF;
			tmp |= RawMemory.readShort(p_address, 2) & 0xFF;
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
