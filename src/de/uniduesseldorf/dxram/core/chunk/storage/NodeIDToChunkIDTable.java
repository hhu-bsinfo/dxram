
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;

import de.uniduesseldorf.dxram.commands.CmdUtils;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.locks.SpinLock;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 * @author Florian Klein
 *         13.02.2014
 */
public final class NodeIDToChunkIDTable {

	// Constants
	public static final byte ENTRY_SIZE = 8;
	public static final byte LID_TABLE_LEVELS = 4;
	private static final byte BITS_PER_LID_LEVEL = 48 / LID_TABLE_LEVELS;
	private static final long LID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_PER_LID_LEVEL) - 1;
	public static final int ENTRIES_PER_LID_LEVEL = (int) Math.pow(2.0, BITS_PER_LID_LEVEL);
	private static final byte BITS_FOR_NID_LEVEL = 16;
	private static final long NID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL) - 1;
	public static final int ENTRIES_FOR_NID_LEVEL = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL);
	public static final int LID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LID_LEVEL + 7;
	public static final int NID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_FOR_NID_LEVEL + 7;
	private static final int LID_LOCK_OFFSET = LID_TABLE_SIZE - 4;
	private static final int NID_LOCK_OFFSET = NID_TABLE_SIZE - 4;

	private static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;

	// Attributes
	private long m_nodeIDTableDirectory;
	private RawMemory m_rawMemory;

	// Constructors
	/**
	 * Creates an instance of CIDTable
	 */
	public NodeIDToChunkIDTable() {}

	// Methods
	/**
	 * Initializes the CIDTable
	 * @param rawMemory The raw memory instance to use for allocation.
	 * @throws MemoryException
	 *             if the CIDTable could not be initialized
	 */
	public void initialize(RawMemory rawMemory) throws MemoryException {
		m_rawMemory = rawMemory;
		m_nodeIDTableDirectory = createNIDTable();

		System.out.println("NodeIDtoChunkIDTable: init success (page directory at: 0x" + Long.toHexString(m_nodeIDTableDirectory) + ")");
	}

	/**
	 * Disengages the CIDTable
	 * @throws MemoryException
	 *             if the CIDTable could not be disengaged
	 */
	public void disengage() throws MemoryException {
		long entry;

		for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
			entry = readEntry(m_nodeIDTableDirectory, i) & BITMASK_ADDRESS;
			if (entry > 0) {
				disengage(entry, LID_TABLE_LEVELS - 1);
				m_rawMemory.free(entry);
			}
		}

		m_nodeIDTableDirectory = 0;
	}

	/**
	 * Disengages a table
	 * @param p_addressTable
	 *            the table
	 * @param p_level
	 *            the table level
	 * @throws MemoryException
	 *             if the table could not be disengaged
	 */
	private void disengage(final long p_addressTable, final int p_level) throws MemoryException {
		long entry;

		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

			if (entry > 0) {
				if (p_level > 0) {
					disengage(entry, p_level - 1);
				}
				m_rawMemory.free(entry);
			}
		}
	}

	/**
	 * Creates the NodeID table
	 * @return the address of the table
	 * @throws MemoryException
	 *             if the table could not be created
	 */
	private long createNIDTable() throws MemoryException {
		long ret;

		ret = m_rawMemory.malloc(NID_TABLE_SIZE);
		m_rawMemory.set(ret, NID_TABLE_SIZE, (byte) 0);

		MemoryStatistic.getInstance().newCIDTable();

		return ret;
	}

	/**
	 * Creates a table
	 * @return the address of the table
	 * @throws MemoryException
	 *             if the table could not be created
	 */
	private long createLIDTable() throws MemoryException {
		long ret;

		ret = m_rawMemory.malloc(LID_TABLE_SIZE);
		m_rawMemory.set(ret, LID_TABLE_SIZE, (byte) 0);

		MemoryStatistic.getInstance().newCIDTable();

		return ret;
	}

	/**
	 * Reads a table entry
	 * @param p_addressTable
	 *            the table
	 * @param p_index
	 *            the index of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be read
	 */
	private long readEntry(final long p_addressTable, final long p_index) throws MemoryException {
		long ret;

		if (p_addressTable == m_nodeIDTableDirectory) {
			ret = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index);
		} else {
			ret = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index);
		}

		return ret;
	}

	/**
	 * Writes a table entry
	 * @param p_addressTable
	 *            the table
	 * @param p_index
	 *            the index of the entry
	 * @param p_entry
	 *            the entry
	 * @throws MemoryException
	 *             if the entry could not be written
	 */
	private void writeEntry(final long p_addressTable, final long p_index, final long p_entry) throws MemoryException {
		m_rawMemory.writeLong(p_addressTable, ENTRY_SIZE * p_index, p_entry);
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	public long get(final long p_chunkID) throws MemoryException {
		long ret;

		readLock(m_nodeIDTableDirectory);

		ret = getEntry(p_chunkID, m_nodeIDTableDirectory, LID_TABLE_LEVELS);

		readUnlock(m_nodeIDTableDirectory);

		return ret;
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressTable
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	private long getEntry(final long p_chunkID, final long p_addressTable, final int p_level) throws MemoryException {
		long ret = 0;
		long index;
		long entry;

		// readLock(p_table);

		if (p_level == LID_TABLE_LEVELS) {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & NID_LEVEL_BITMASK;
		} else {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & LID_LEVEL_BITMASK;
		}
		
		if (p_level > 0) {
			entry = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
			if (entry > 0) {
				ret = getEntry(p_chunkID, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			entry = readEntry(p_addressTable, index);
			ret = entry;
		}

		// readUnlock(p_table);

		return ret;
	}

	/**
	 * Sets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressChunk
	 *            the address of the chunk
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	public void set(final long p_chunkID, final long p_addressChunk) throws MemoryException {
		setEntry(p_chunkID, p_addressChunk, m_nodeIDTableDirectory, LID_TABLE_LEVELS);
	}

	/**
	 * Sets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressChunk
	 *            the address of the chunk
	 * @param p_addressTable
	 *            the address of the current CID table
	 * @param p_level
	 *            the table level
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	private void setEntry(final long p_chunkID, final long p_addressChunk, final long p_addressTable, final int p_level) throws MemoryException {
		long index;
		long entry;

		if (p_level == LID_TABLE_LEVELS) {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & NID_LEVEL_BITMASK;
		} else {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & LID_LEVEL_BITMASK;
		}
		if (p_level > 0) {
			writeLock(p_addressTable);

			// Read table entry
			entry = readEntry(p_addressTable, index);
			if (entry == 0) {
				entry = createLIDTable();
				writeEntry(p_addressTable, index, entry);
			}

			writeUnlock(p_addressTable);

			if (entry > 0) {
				// move on to next table
				setEntry(p_chunkID, p_addressChunk, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			writeLock(p_addressTable);

			// Set the level 0 entry
			// valid and active entry, delete flag 0
			writeEntry(p_addressTable, index, p_addressChunk);

			writeUnlock(p_addressTable);
		}
	}

	/**
	 * Locks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	private void readLock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			m_rawMemory.readLock(p_address + NID_LOCK_OFFSET);
		} else {
			m_rawMemory.readLock( p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	private void readUnlock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			m_rawMemory.readUnlock(p_address + NID_LOCK_OFFSET);
		} else {
			m_rawMemory.readUnlock(p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private void writeLock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			m_rawMemory.writeLock(p_address + NID_LOCK_OFFSET);
		} else {
			m_rawMemory.writeLock(p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private void writeUnlock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			m_rawMemory.writeUnlock(p_address + NID_LOCK_OFFSET);
		} else {
			m_rawMemory.writeUnlock(p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Prints debug informations
	 */
	public void printDebugInfos() {
		StringBuilder infos;
		int[] count;

		count = new int[LID_TABLE_LEVELS + 1];

		countTables(m_nodeIDTableDirectory, LID_TABLE_LEVELS, count);

		infos = new StringBuilder();
		infos.append("\nCIDTable:\n");
		for (int i = LID_TABLE_LEVELS; i >= 0; i--) {
			infos.append("\t" + count[i] + " table(s) on level " + i + "\n");
		}

		System.out.println(infos);
	}

	/**
	 * Counts the subtables
	 * @param p_addressTable
	 *            the current table
	 * @param p_level
	 *            the level of the table
	 * @param p_count
	 *            the table counts
	 */
	private void countTables(final long p_addressTable, final int p_level, final int[] p_count) {
		long entry;

		p_count[p_level]++;

		if (p_level == LID_TABLE_LEVELS) {
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				try {
					entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;
				} catch (final MemoryException e) {
					entry = 0;
				}

				if (entry > 0) {
					countTables(entry, p_level - 1, p_count);
				}
			}
		} else {
			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				try {
					entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;
				} catch (final MemoryException e) {
					entry = 0;
				}

				if (entry > 0) {
					if (p_level > 1) {
						countTables(entry, p_level - 1, p_count);
					} else {
						p_count[0]++;
					}
				}
			}
		}
	}
}
