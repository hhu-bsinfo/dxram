
package de.hhu.bsinfo.dxram.mem;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.soh.SmallObjectHeap;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 * @author Florian Klein
 *         13.02.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class CIDTable {

	public static final byte ENTRY_SIZE = 5;
	public static final byte LID_TABLE_LEVELS = 4;
	private static final byte BITS_PER_LID_LEVEL = 48 / LID_TABLE_LEVELS;
	private static final long LID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_PER_LID_LEVEL) - 1;
	public static final int ENTRIES_PER_LID_LEVEL = (int) Math.pow(2.0, BITS_PER_LID_LEVEL);
	private static final byte BITS_FOR_NID_LEVEL = 16;
	private static final long NID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL) - 1;
	public static final int ENTRIES_FOR_NID_LEVEL = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL);
	public static final int LID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LID_LEVEL + 7;
	public static final int NID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_FOR_NID_LEVEL + 7;

	protected static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;
	protected static final long BIT_FLAG = 0x8000000000L;
	protected static final long FULL_FLAG = BIT_FLAG;
	protected static final long DELETED_FLAG = BIT_FLAG;

	private short m_ownNodeID = -1;
	private long m_addressTableDirectory = -1;
	private SmallObjectHeap m_rawMemory;

	private LIDStore m_store;

	private LoggerComponent m_logger;
	private StatisticsComponent m_statistics;

	private MemoryStatisticsRecorderIDs m_statisticsRecorderIDs;

	private AtomicLong m_nextLocalID;

	/**
	 * Creates an instance of CIDTable
	 * @param p_ownNodeID
	 *            ID of the current node.
	 * @param p_statistics
	 *            Statistics component for recording.
	 * @param p_statisticsRecorderIDs
	 *            Metadata for statistics recording
	 * @param p_logger
	 *            Logger component for logging.
	 */
	public CIDTable(final short p_ownNodeID, final StatisticsComponent p_statistics,
			final MemoryStatisticsRecorderIDs p_statisticsRecorderIDs,
			final LoggerComponent p_logger) {
		m_ownNodeID = p_ownNodeID;
		m_statistics = p_statistics;
		m_statisticsRecorderIDs = p_statisticsRecorderIDs;
		m_logger = p_logger;
	}

	// Methods
	/**
	 * Initializes the CIDTable
	 * @param p_rawMemory
	 *            The raw memory instance to use for allocation.
	 */
	public void initialize(final SmallObjectHeap p_rawMemory) {
		m_rawMemory = p_rawMemory;
		m_addressTableDirectory = createNIDTable();

		m_store = new LIDStore();
		m_nextLocalID = new AtomicLong(1);

		m_logger.info(getClass(),
				"CIDTable: init success (page directory at: 0x" + Long.toHexString(m_addressTableDirectory) + ")");
	}

	/**
	 * Disengages the CIDTable
	 */
	public void disengage() {
		m_store = null;
		m_nextLocalID = null;

		m_addressTableDirectory = -1;
	}

	/**
	 * Get a free LID from the CIDTable
	 * @return a free LID and version, or -1 if there is none
	 */
	public long getFreeLID() {
		long ret = -1;

		if (m_store != null) {
			ret = m_store.get();
		}

		// If no free ID exist, get next local ID
		if (ret == -1) {
			ret = m_nextLocalID.getAndIncrement();
		}

		return ret;
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @return the entry. 0 for invalid/unused.
	 */
	public long get(final long p_chunkID) {
		long ret;

		ret = getEntry(p_chunkID, m_addressTableDirectory, LID_TABLE_LEVELS);

		return ret;
	}

	/**
	 * Sets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressChunk
	 *            the address of the chunk
	 */
	public void set(final long p_chunkID, final long p_addressChunk) {
		setEntry(p_chunkID, p_addressChunk, m_addressTableDirectory, LID_TABLE_LEVELS);
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_flagZombie
	 *            Flag the deleted entry as a zombie or not zombie i.e. fully deleted.
	 * @return The address of the chunk which was removed from the table.
	 */
	public long delete(final long p_chunkID, final boolean p_flagZombie) {
		long ret;
		ret = deleteEntry(p_chunkID, m_addressTableDirectory, LID_TABLE_LEVELS, p_flagZombie);
		return ret;
	}

	/**
	 * Puts the LocalID of a deleted migrated Chunk to LIDStore
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @return m_cidTable
	 */
	public boolean putChunkIDForReuse(final long p_chunkID) {
		return m_store.put(ChunkID.getLocalID(p_chunkID));
	}

	/**
	 * Returns the ChunkID ranges of all locally stored Chunks
	 * @return the ChunkID ranges in an ArrayList
	 */
	public ArrayList<Long> getCIDRangesOfAllLocalChunks() {
		ArrayList<Long> ret = null;
		long entry;
		long intervalStart;
		long intervalEnd;

		if (m_store != null) {
			ret = new ArrayList<Long>();
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				entry = readEntry(m_addressTableDirectory, i) & BITMASK_ADDRESS;
				if (entry > 0) {
					if (i == (m_ownNodeID & 0xFFFF)) {
						ret.addAll(getAllRanges((long) i << 48,
								readEntry(m_addressTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS,
								LID_TABLE_LEVELS - 1));
					}
				}
			}
		}

		// compress intervals
		if (ret.size() >= 2) {
			if (ret.size() % 2 != 0) {
				// throw new MemoryException("internal error in getChunkIDRangesOfAllChunks");
				// System.out.println("error: in ChunkIDRange list");
			} else {
				for (int i = 0; i < ret.size() - 2; i += 2) {
					intervalEnd = ChunkID.getLocalID(ret.get(i + 1));
					intervalStart = ChunkID.getLocalID(ret.get(i + 2));

					// can we merge intervals?
					if (intervalEnd + 1 == intervalStart) {
						// System.out.println(" remove el.");
						ret.remove(i + 1);
						ret.remove(i + 1);
						i -= 2;
					}
				}
			}
		}

		return ret;
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 */
	public ArrayList<Long> getCIDOfAllMigratedChunks() {
		ArrayList<Long> ret = null;
		long entry;

		if (m_store != null) {
			ret = new ArrayList<Long>();
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				entry = readEntry(m_addressTableDirectory, i) & BITMASK_ADDRESS;
				if (entry > 0 && i != (m_ownNodeID & 0xFFFF)) {
					ret.addAll(getAllEntries((long) i << 48, readEntry(m_addressTableDirectory,
							i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1));
				}
			}
		}

		return ret;
	}

	// -----------------------------------------------------------------------------------------

	/**
	 * Creates the NodeID table
	 * @return the address of the table
	 */
	private long createNIDTable() {
		long ret;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_createNIDTable,
				NID_TABLE_SIZE);

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc, NID_TABLE_SIZE);
		ret = m_rawMemory.malloc(NID_TABLE_SIZE);
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc);
		m_rawMemory.set(ret, NID_TABLE_SIZE, (byte) 0);

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_createNIDTable);

		return ret;
	}

	/**
	 * Creates a table
	 * @return the address of the table
	 */
	private long createLIDTable() {
		long ret;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_createLIDTable,
				LID_TABLE_SIZE);

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc, LID_TABLE_SIZE);
		ret = m_rawMemory.malloc(LID_TABLE_SIZE);
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_malloc);
		m_rawMemory.set(ret, LID_TABLE_SIZE, (byte) 0);

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_createLIDTable);

		return ret;
	}

	/**
	 * Reads a table entry
	 * @param p_addressTable
	 *            the table
	 * @param p_index
	 *            the index of the entry
	 * @return the entry
	 */
	private long readEntry(final long p_addressTable, final long p_index) {
		long ret;

		if (p_addressTable == m_addressTableDirectory) {
			ret = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFFFFFFL;
		} else {
			ret = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFFFFFFL;
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
	 */
	private void writeEntry(final long p_addressTable, final long p_index, final long p_entry) {
		long value;

		value = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFF0000000000L;
		value += p_entry & 0xFFFFFFFFFFL;

		m_rawMemory.writeLong(p_addressTable, ENTRY_SIZE * p_index, value);
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
	 */
	private long getEntry(final long p_chunkID, final long p_addressTable, final int p_level) {
		long ret = 0;
		long index;
		long entry;

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
			ret = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
		}

		return ret;
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
	 */
	private void setEntry(final long p_chunkID, final long p_addressChunk, final long p_addressTable,
			final int p_level) {
		long index;
		long entry;

		if (p_level == LID_TABLE_LEVELS) {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & NID_LEVEL_BITMASK;
		} else {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & LID_LEVEL_BITMASK;
		}
		if (p_level > 0) {
			// Read table entry
			entry = readEntry(p_addressTable, index);
			if (entry == 0) {
				entry = createLIDTable();
				writeEntry(p_addressTable, index, entry);
			}

			if (entry > 0) {
				// move on to next table
				setEntry(p_chunkID, p_addressChunk, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			// Set the level 0 entry
			// valid and active entry, delete flag 0
			writeEntry(p_addressTable, index, p_addressChunk & BITMASK_ADDRESS);
		}
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressTable
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @param p_flagZombie
	 *            flag the deleted entry as zombie i.e. keep the chunk
	 *            allocated but remove it from the table index.
	 * @return the entry
	 */
	private long deleteEntry(final long p_chunkID, final long p_addressTable, final int p_level,
			final boolean p_flagZombie) {
		long ret = -1;
		long index;
		long entry;

		if (p_level == LID_TABLE_LEVELS) {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & NID_LEVEL_BITMASK;
		} else {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & LID_LEVEL_BITMASK;
		}
		if (p_level > 0) {
			// Read table entry
			entry = readEntry(p_addressTable, index);
			if ((entry & FULL_FLAG) > 0) {
				// Delete full flag
				entry &= ~FULL_FLAG;
				writeEntry(p_addressTable, index, entry);
			}

			if ((entry & BITMASK_ADDRESS) > 0) {
				// Delete entry in the following table
				ret = deleteEntry(p_chunkID, entry & BITMASK_ADDRESS, p_level - 1, p_flagZombie);
			}
		} else {
			// Read the level 0 entry
			ret = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
			// Delete the level 0 entry
			// invalid + active address but deleted flag 1
			// -> zombie entry
			if (p_flagZombie) {
				writeEntry(p_addressTable, index, ret | DELETED_FLAG);
			} else {
				// delete flag cleared, but address is 0 -> free entry
				writeEntry(p_addressTable, index, 0);
			}
		}

		return ret;
	}

	/**
	 * Adds all ChunkID ranges to an ArrayList
	 * @param p_unfinishedCID
	 *            the unfinished ChunkID
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the current table level
	 * @return the ArrayList
	 */
	private ArrayList<Long> getAllRanges(final long p_unfinishedCID, final long p_table, final int p_level) {
		ArrayList<Long> ret;
		long entry;
		int range = 0;

		ret = new ArrayList<Long>();
		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_table, i);
			if (entry > 0) {

				if ((entry & DELETED_FLAG) == 0) {

					if (p_level > 0) {
						ret.addAll(getAllRanges(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level),
								entry & BITMASK_ADDRESS, p_level - 1));
					} else {
						if (range == 0) {
							range = 1;
							ret.add(p_unfinishedCID + i);
						} else if (range == 1) {
							ret.remove(ret.size() - 1);
						}
						ret.add(p_unfinishedCID + i);
					}
				}
			} else {
				range = 0;
			}
		}

		return ret;
	}

	/**
	 * Adds all ChunkIDs to an ArrayList
	 * @param p_unfinishedCID
	 *            the unfinished ChunkID
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the current table level
	 * @return the ArrayList
	 */
	private ArrayList<Long> getAllEntries(final long p_unfinishedCID, final long p_table, final int p_level) {
		ArrayList<Long> ret;
		long entry;

		// System.out.println("Entering with " + Long.toHexString(p_unfinishedCID));

		ret = new ArrayList<Long>();
		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_table, i);
			if (entry > 0) {
				if (p_level > 0) {
					ret.addAll(getAllEntries(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level),
							entry & BITMASK_ADDRESS, p_level - 1));
				} else {
					ret.add(p_unfinishedCID + i);
				}
			}
		}

		return ret;
	}

	/**
	 * Prints debug informations
	 */
	public void printDebugInfos() {
		StringBuilder infos;
		int[] count;

		count = new int[LID_TABLE_LEVELS + 1];

		countTables(m_addressTableDirectory, LID_TABLE_LEVELS, count);

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
				entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

				if (entry > 0) {
					countTables(entry, p_level - 1, p_count);
				}
			}
		} else {
			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

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

	// Classes
	/**
	 * Stores free LocalIDs
	 * @author Florian Klein
	 *         30.04.2014
	 */
	private final class LIDStore {

		// Constants
		private static final int STORE_CAPACITY = 1000;

		// Attributes
		private final long[] m_localIDs;
		private int m_position;
		// available free lid elements stored in our array
		private int m_count;
		// This counts the total available lids in the array
		// as well as elements that are still allocated
		// (because they don't fit into the local array anymore)
		// but not valid -> zombies
		private volatile long m_overallCount;

		// Constructors
		/**
		 * Creates an instance of LIDStore
		 */
		private LIDStore() {
			m_localIDs = new long[STORE_CAPACITY];
			m_position = 0;
			m_count = 0;

			m_overallCount = 0;
		}

		// Methods
		/**
		 * Gets a free LocalID
		 * @return a free LocalID
		 */
		public long get() {
			long ret = -1;

			if (m_overallCount > 0) {
				if (m_count == 0) {
					fill();
				}

				if (m_count > 0) {
					ret = m_localIDs[m_position];

					m_position = (m_position + 1) % m_localIDs.length;
					m_count--;
					m_overallCount--;
				}
			}

			return ret;
		}

		/**
		 * Puts a free LocalID
		 * @param p_localID
		 *            a LocalID
		 * @return True if adding an entry to our local ID store was successful, false otherwise.
		 */
		public boolean put(final long p_localID) {
			boolean ret;

			if (m_count < m_localIDs.length) {
				m_localIDs[m_position + m_count] = p_localID;

				m_count++;
				ret = true;
			} else {
				ret = false;
			}

			m_overallCount++;

			return ret;
		}

		/**
		 * Fills the store
		 */
		private void fill() {
			findFreeLIDs();
		}

		/**
		 * Finds free LIDs in the CIDTable
		 */
		private void findFreeLIDs() {
			findFreeLIDs(readEntry(m_addressTableDirectory, m_ownNodeID & NID_LEVEL_BITMASK) & BITMASK_ADDRESS,
					LID_TABLE_LEVELS - 1, 0);
		}

		/**
		 * Finds free LIDs in the CIDTable
		 * @param p_addressTable
		 *            the table
		 * @param p_level
		 *            the table level
		 * @param p_offset
		 *            the offset of the LID
		 * @return true if free LIDs were found, false otherwise
		 */
		private boolean findFreeLIDs(final long p_addressTable, final int p_level, final long p_offset) {
			boolean ret = false;
			long chunkID;
			long nodeID;
			long localID;
			long entry;

			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				// Read table entry
				entry = readEntry(p_addressTable, i);

				if (p_level > 0) {
					if (entry > 0) {
						// Get free LocalID in the next table
						if (!findFreeLIDs(entry & BITMASK_ADDRESS, p_level - 1, i << BITS_PER_LID_LEVEL * p_level)) {
							// Mark the table as full
							entry |= FULL_FLAG;
							writeEntry(p_addressTable, i, entry);
						} else {
							ret = true;
						}
					}
				} else {
					// check if we got an entry referencing a zombie
					if ((entry & DELETED_FLAG) > 0 && (entry & BITMASK_ADDRESS) > 0) {
						nodeID = m_ownNodeID;
						localID = p_offset + i;

						chunkID = nodeID << 48;
						chunkID = chunkID + localID;

						// cleanup zombie in table
						writeEntry(p_addressTable, i, 0);

						m_localIDs[m_position + m_count] = localID;
						m_count++;

						ret = true;
					}
				}

				if (m_count == m_localIDs.length || m_count == m_overallCount) {
					break;
				}
			}

			return ret;
		}
	}
}
