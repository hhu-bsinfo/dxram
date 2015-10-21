
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.LIDElement;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;
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

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 */
	private MemoryManager() {}

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

	/**
	 * Puts a Chunk in the memory
	 * @param p_chunk
	 *            the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be put
	 */
	public static void put(final Chunk p_chunk) throws MemoryException {
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
		address = CIDTable.get(chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address <= 0) {
			address = RawMemory.malloc(totalChunkSize);
			CIDTable.set(chunkID, address);
		} else {
			// check if we have to expand
			long oldSize;

			oldSize = RawMemory.getSize(address);
			if (oldSize < totalChunkSize) {
				// re-allocate
				RawMemory.free(address);
				address = RawMemory.malloc(totalChunkSize);
				CIDTable.set(chunkID, address);
			}
		}

		// set the size of the version header
		RawMemory.setCustomState(address, sizeVersion - 1);
		writeVersion(address, version, sizeVersion);
		RawMemory.writeBytes(address, sizeVersion, p_chunk.getData().array());
		
		RawMemory.dump(new File(Long.toString(address)), address - 1, p_chunk.getSize() + 20);
	}

	/**
	 * Gets the Chunk for the given ChunkID from the memory
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the corresponding Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public static Chunk get(final long p_chunkID) throws MemoryException {
		Chunk ret = null;
		long address;

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address > 0) {
			int version;
			int sizeVersion;
			byte[] data;

			sizeVersion = RawMemory.getCustomState(address) + 1;
			version = readVersion(address, sizeVersion);

			// make sure to skip version data
			data = RawMemory.readBytes(address, sizeVersion);

			ret = new Chunk(p_chunkID, data, version);
		}
		
		RawMemory.dump(new File(Long.toString(address) + "_2"), address - 1, ret.getSize() + 20);

		return ret;
	}

	/**
	 * Returns whether this Chunk is stored locally or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk is stored locally or not
	 * @throws MemoryException
	 *             if the Chunk could not be checked
	 */
	public static boolean isResponsible(final long p_chunkID) throws MemoryException {
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
	public static boolean wasMigrated(final long p_chunkID) {
		return ChunkID.getCreatorID(p_chunkID) != NodeID.getLocalNodeID();
	}

	/**
	 * Removes a Chunk from the memory
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public static void remove(final long p_chunkID) throws MemoryException {
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
			RawMemory.dump(new File(Long.toString(chunkCleanup.second()) + "_3"), chunkCleanup.second() - 1, 200);
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

	/**
	 * Get the necessary storage size for the given version number.
	 * @param p_version
	 *            Version number to get the storage size for.
	 * @return Storage size for the specified version or -1 if size not supported.
	 */
	protected static byte getSizeVersion(final int p_version) {
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
	protected static void writeVersion(final long p_address, final int p_version, final int p_size)
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
	protected static int readVersion(final long p_address, final int p_size) throws MemoryException {
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
