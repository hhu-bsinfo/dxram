
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
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
	public static long getNextLocalID() {
		long ret = -1;

		// Try to get free ID from the CIDTable
		try {
			ret = CIDTable.getFreeCID();
		} catch (final DXRAMException e) {}

		// If no free ID exist, get next local ID
		if (ret == -1) {
			ret = m_nextLocalID.getAndIncrement();
		} else {
			ret = ChunkID.getLocalID(ret);
		}

		return ret;
	}

	/**
	 * Gets the next free local IDs
	 * @param p_count
	 *            the number of free local IDs
	 * @return the next free local IDs
	 */
	public static long[] getNextLocalIDs(final int p_count) {
		long[] ret = null;
		long lid;

		lid = m_nextLocalID.getAndAdd(p_count);
		if (lid != -1) {
			ret = new long[p_count];
			for (int i = 0; i < p_count; i++) {
				ret[i] = lid + i;
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

		chunkID = p_chunk.getChunkID();
		version = p_chunk.getVersion();

		// Get the address from the CIDTable
		address = CIDTable.get(chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address <= 0) {
			address = RawMemory.malloc(p_chunk.getSize() + Integer.BYTES);
			CIDTable.set(chunkID, address);
		}

		RawMemory.writeVersion(address, version);
		RawMemory.writeBytes(address, p_chunk.getData().array());
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
		int version;
		long address;

		// Get the address from the CIDTable
		address = CIDTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address > 0) {
			version = RawMemory.readVersion(address);
			ret = new Chunk(p_chunkID, RawMemory.readBytes(address), version);
		}

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
		long address;

		// Get and delete the address from the CIDTable
		address = CIDTable.delete(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		if (address > 0) {
			RawMemory.free(address);
		} else {
			throw new MemoryException("MemoryManager.remove failed");
		}
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
}
