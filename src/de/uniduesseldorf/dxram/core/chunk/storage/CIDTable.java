
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;

import de.uniduesseldorf.dxram.commands.CmdUtils;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.locks.JNILock;
import de.uniduesseldorf.dxram.utils.locks.SpinLock;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 * @author Florian Klein
 *         13.02.2014
 */
public final class CIDTable {

	// Constants
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
	private static final int LID_LOCK_OFFSET = LID_TABLE_SIZE - 4;
	private static final int NID_LOCK_OFFSET = NID_TABLE_SIZE - 4;

	private static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;
	private static final long BIT_FLAG = 0x8000000000L;
	private static final long FULL_FLAG = BIT_FLAG;
	private static final long DELETED_FLAG = BIT_FLAG;

	// Attributes
	private static long m_nodeIDTableDirectory;
	private static long m_memoryBase;

	private static LIDStore m_store;

	private static Defragmenter m_defragmenter;

	// Constructors
	/**
	 * Creates an instance of CIDTable
	 */
	private CIDTable() {}

	// Methods
	/**
	 * Initializes the CIDTable
	 * @throws MemoryException
	 *             if the CIDTable could not be initialized
	 */
	public static void initialize() throws MemoryException {
		m_memoryBase = RawMemory.getMemoryBase();
		m_nodeIDTableDirectory = createNIDTable();

		m_store = new LIDStore();

		m_defragmenter = new Defragmenter();
		// TODO: new Thread(m_defragmenter).start();

		System.out.println("CIDTable: init success (page directory at: 0x" + Long.toHexString(m_nodeIDTableDirectory) + ")");
	}

	/**
	 * Disengages the CIDTable
	 * @throws MemoryException
	 *             if the CIDTable could not be disengaged
	 */
	public static void disengage() throws MemoryException {
		long entry;

		m_defragmenter.stop();
		m_defragmenter = null;

		m_store = null;

		for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
			entry = readEntry(m_nodeIDTableDirectory, i) & BITMASK_ADDRESS;
			if (entry > 0) {
				disengage(entry, LID_TABLE_LEVELS - 1);
				RawMemory.free(entry);
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
	private static void disengage(final long p_addressTable, final int p_level) throws MemoryException {
		long entry;

		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

			if (entry > 0) {
				if (p_level > 0) {
					disengage(entry, p_level - 1);
				}
				RawMemory.free(entry);
			}
		}
	}

	/**
	 * Creates the NodeID table
	 * @return the address of the table
	 * @throws MemoryException
	 *             if the table could not be created
	 */
	private static long createNIDTable() throws MemoryException {
		long ret;

		ret = RawMemory.malloc(NID_TABLE_SIZE);
		RawMemory.set(ret, NID_TABLE_SIZE, (byte) 0);

		MemoryStatistic.getInstance().newCIDTable();

		return ret;
	}

	/**
	 * Creates a table
	 * @return the address of the table
	 * @throws MemoryException
	 *             if the table could not be created
	 */
	private static long createLIDTable() throws MemoryException {
		long ret;

		ret = RawMemory.malloc(LID_TABLE_SIZE);
		RawMemory.set(ret, LID_TABLE_SIZE, (byte) 0);

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
	private static long readEntry(final long p_addressTable, final long p_index) throws MemoryException {
		long ret;

		if (p_addressTable == m_nodeIDTableDirectory) {
			ret = RawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFFFFFFL;
		} else {
			ret = RawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFFFFFFL;
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
	private static void writeEntry(final long p_addressTable, final long p_index, final long p_entry) throws MemoryException {
		long value;

		value = RawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFF0000000000L;
		value += p_entry & 0xFFFFFFFFFFL;

		RawMemory.writeLong(p_addressTable, ENTRY_SIZE * p_index, value);
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	protected static long get(final long p_chunkID) throws MemoryException {
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
	private static long getEntry(final long p_chunkID, final long p_addressTable, final int p_level) throws MemoryException {
		long ret = 0;
		long index;
		long entry;

		// readLock(p_table);

		if (p_level == LID_TABLE_LEVELS) {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & NID_LEVEL_BITMASK;
		} else {
			index = p_chunkID >> BITS_PER_LID_LEVEL * p_level & LID_LEVEL_BITMASK;
		}
		entry = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
		if (p_level > 0) {
			if (entry > 0) {
				ret = getEntry(p_chunkID, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
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
	public static void set(final long p_chunkID, final long p_addressChunk) throws MemoryException {
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
	private static void setEntry(final long p_chunkID, final long p_addressChunk, final long p_addressTable, final int p_level) throws MemoryException {
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
			writeEntry(p_addressTable, index, p_addressChunk & BITMASK_ADDRESS);

			writeUnlock(p_addressTable);
		}
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	protected static long delete(final long p_chunkID) throws MemoryException {
		long ret;
		int version;
		int sizeVersion;

		ret = deleteEntry(p_chunkID, m_nodeIDTableDirectory, LID_TABLE_LEVELS);

		// read version
		sizeVersion = RawMemory.getCustomState(ret) + 1;
		version = MemoryManager.readVersion(ret, sizeVersion);

		m_store.put(ChunkID.getLocalID(p_chunkID), version);

		return ret;
	}

	/**
	 * Puts the LocalID of a deleted migrated Chunk to LIDStore
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_version
	 *            the version of the entry
	 */
	protected static void putChunkIDForReuse(final long p_chunkID, final int p_version) {
		m_store.put(ChunkID.getLocalID(p_chunkID), p_version);
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_addressTable
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be deleted
	 */
	private static long deleteEntry(final long p_chunkID, final long p_addressTable, final int p_level) throws MemoryException {
		long ret = -1;
		long index;
		long entry;

		writeLock(p_addressTable);

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
				ret = deleteEntry(p_chunkID, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			// Read the level 0 entry
			ret = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
			// Delete the level 0 entry
			writeEntry(p_addressTable, index, DELETED_FLAG);
		}

		writeUnlock(p_addressTable);

		return ret;
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public static ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
		ArrayList<Long> ret = null;
		long entry;

		if (m_store != null) {

			readLock(m_nodeIDTableDirectory);

			ret = new ArrayList<Long>();
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				entry = readEntry(m_nodeIDTableDirectory, i) & BITMASK_ADDRESS;
				if (entry > 0 && i != (NodeID.getLocalNodeID() & 0xFFFF)) {
					ret.addAll(getAllEntries((long) i << 48, readEntry(m_nodeIDTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1));
				}
			}

			readUnlock(m_nodeIDTableDirectory);
		}

		return ret;
	}

	/**
	 * Returns the ChunkID ranges of all locally stored Chunks
	 * @return the ChunkID ranges in an ArrayList
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public static ArrayList<Long> getCIDrangesOfAllLocalChunks() throws MemoryException {
		ArrayList<Long> ret = null;
		long entry;
		long intervalStart;
		long intervalEnd;

		if (m_store != null) {

			readLock(m_nodeIDTableDirectory);

			ret = new ArrayList<Long>();
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				entry = readEntry(m_nodeIDTableDirectory, i) & BITMASK_ADDRESS;
				if (entry > 0) {
					if (i == (NodeID.getLocalNodeID() & 0xFFFF)) {
						ret.addAll(getAllRanges((long) i << 48, readEntry(m_nodeIDTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS,
								LID_TABLE_LEVELS - 1));
					}
				}
			}

			readUnlock(m_nodeIDTableDirectory);
		}

		/*
		 * // dump ChunkID ranges
		 * System.out.println("getCIDrangesOfAllChunks: DUMP ChunkIDRanges");
		 * for (int i=0; i<ret.size(); i++) {
		 * System.out.println("   i="+i+", el: "+CmdUtils.getLIDfromCID(ret.get(i)));
		 * }
		 */
		// compress intervals

		if (ret.size() >= 2) {
			if (ret.size() % 2 != 0) {
				throw new MemoryException("internal error in getChunkIDRangesOfAllChunks");
				// System.out.println("error: in ChunkIDRange list");
			} else {
				for (int i = 0; i < ret.size() - 2; i += 2) {
					intervalEnd = CmdUtils.getLIDfromCID(ret.get(i + 1));
					intervalStart = CmdUtils.getLIDfromCID(ret.get(i + 2));

					// can we melt intervals?
					if (intervalEnd + 1 == intervalStart) {
						System.out.println("   remove el.");
						ret.remove(i + 1);
						ret.remove(i + 1);
						i -= 2;
					}
				}
			}
		}
		/*
		 * // dump ChunkID ranges
		 * System.out.println("getCIDrangesOfAllChunks: DUMP ChunkIDRanges after compression");
		 * Iterator<Long> il = ret.iterator();
		 * while (il.hasNext()) {
		 * System.out.println("   el: "+CmdUtils.getLIDfromCID(il.next()));
		 * }
		 */
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
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	private static ArrayList<Long> getAllRanges(final long p_unfinishedCID, final long p_table, final int p_level) throws MemoryException {
		ArrayList<Long> ret;
		long entry;
		int range = 0;

		ret = new ArrayList<Long>();
		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_table, i);
			if (entry > 0) {

				if ((entry & DELETED_FLAG) == 0) {

					if (p_level > 0) {
						ret.addAll(getAllRanges(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level), entry & BITMASK_ADDRESS, p_level - 1));
					} else {
						if (range == 0) {
							range = 1;
						} else if (range == 1) {
							range = 2;
						} else if (range == 2) {
							ret.remove(ret.size() - 1);
						}
						ret.add(p_unfinishedCID + i);
					}
				} else {
					range = 0;
				}
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
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	private static ArrayList<Long> getAllEntries(final long p_unfinishedCID, final long p_table, final int p_level) throws MemoryException {
		ArrayList<Long> ret;
		long entry;

		// System.out.println("Entering with " + Long.toHexString(p_unfinishedCID));

		ret = new ArrayList<Long>();
		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			entry = readEntry(p_table, i);
			if (entry > 0) {
				if (p_level > 0) {
					ret.addAll(getAllEntries(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level), entry & BITMASK_ADDRESS, p_level - 1));
				} else {
					ret.add(p_unfinishedCID + i);
				}
			}
		}

		return ret;
	}

	/**
	 * Get a free LID from the CIDTable
	 * @return a free LID and version, or null if there is none
	 * @throws MemoryException
	 *             if the CIDTable could not be accessed
	 */
	protected static LIDElement getFreeLID() throws MemoryException {
		LIDElement ret = null;

		if (m_store != null) {
			ret = m_store.get();
		}

		return ret;
	}

	/**
	 * Locks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void readLock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			JNILock.readLock(m_memoryBase + p_address + NID_LOCK_OFFSET);
		} else {
			JNILock.readLock(m_memoryBase + p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void readUnlock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			JNILock.readUnlock(m_memoryBase + p_address + NID_LOCK_OFFSET);
		} else {
			JNILock.readUnlock(m_memoryBase + p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void writeLock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			JNILock.writeLock(m_memoryBase + p_address + NID_LOCK_OFFSET);
		} else {
			JNILock.writeLock(m_memoryBase + p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void writeUnlock(final long p_address) {
		if (p_address == m_nodeIDTableDirectory) {
			JNILock.writeUnlock(m_memoryBase + p_address + NID_LOCK_OFFSET);
		} else {
			JNILock.writeUnlock(m_memoryBase + p_address + LID_LOCK_OFFSET);
		}
	}

	/**
	 * Defragments all Tables
	 * @return the time of the defragmentation
	 * @throws MemoryException
	 *             if the tables could not be defragmented
	 */
	public static long defragmentAll() throws MemoryException {
		return m_defragmenter.defragmentAll();
	}

	/**
	 * Prints debug informations
	 */
	public static void printDebugInfos() {
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
	private static void countTables(final long p_addressTable, final int p_level, final int[] p_count) {
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

	// Classes
	/**
	 * Stores free LocalIDs
	 * @author Florian Klein
	 *         30.04.2014
	 */
	private static final class LIDStore {

		// Constants
		private static final int STORE_CAPACITY = 100000;

		// Attributes
		private final LIDElement[] m_localIDs;
		private int m_position;
		private int m_count;
		private volatile long m_overallCount;

		private Lock m_lock;

		// Constructors
		/**
		 * Creates an instance of LIDStore
		 */
		private LIDStore() {
			m_localIDs = new LIDElement[STORE_CAPACITY];
			m_position = 0;
			m_count = 0;

			m_overallCount = 0;

			m_lock = new SpinLock();
		}

		// Methods
		/**
		 * Gets a free LocalID
		 * @return a free LocalID
		 */
		public LIDElement get() {
			LIDElement ret = null;

			if (m_overallCount > 0) {
				m_lock.lock();

				// TODO: Removed until version is stored in CIDTable
				/*-if (m_count == 0) {
					fill();
				}*/

				if (m_count > 0) {
					ret = m_localIDs[m_position];

					m_position = (m_position + 1) % m_localIDs.length;
					m_count--;
					m_overallCount--;
				}

				m_lock.unlock();
			}

			return ret;
		}

		/**
		 * Puts a free LocalID
		 * @param p_localID
		 *            a LocalID
		 * @param p_version
		 *            a version
		 */
		public void put(final long p_localID, final int p_version) {
			m_lock.lock();

			if (m_count < m_localIDs.length) {
				m_localIDs[m_position + m_count] = new LIDElement(p_localID, p_version);

				m_count++;
			}
			m_overallCount++;

			m_lock.unlock();
		}

		/**
		 * Fills the store
		 */
		@SuppressWarnings("unused")
		private void fill() {
			try {
				findFreeLIDs();
			} catch (final MemoryException e) {}
		}

		/**
		 * Finds free LIDs in the CIDTable
		 * @throws MemoryException
		 *             if the CIDTable could not be accessed
		 */
		private void findFreeLIDs() throws MemoryException {
			findFreeLIDs(readEntry(m_nodeIDTableDirectory, NodeID.getLocalNodeID() & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1, 0);
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
		 * @throws MemoryException
		 *             if the CIDTable could not be accessed
		 */
		private boolean findFreeLIDs(final long p_addressTable, final int p_level, final long p_offset) throws MemoryException {
			boolean ret = false;
			long entry;

			writeLock(p_addressTable);

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
					if ((entry & DELETED_FLAG) > 0) {
						writeEntry(p_addressTable, i, 0);

						// TODO: Version is unknown, because it is not stored in CIDTable and removed from RAWMemory
						m_localIDs[m_position + m_count] = new LIDElement(ChunkID.getLocalID(p_offset + i), -1);
						m_count++;

						ret = true;
					}
				}

				if (m_count == m_localIDs.length || m_count == m_overallCount) {
					break;
				}
			}

			writeUnlock(p_addressTable);

			return ret;
		}

	}

	/**
	 * Stores free LocalIDs
	 * @author Florian Klein
	 *         30.04.2014
	 */
	public static final class LIDElement {

		// Attributes
		private long m_localID;
		private int m_version;

		// Constructors
		/**
		 * Creates an instance of LIDElement
		 * @param p_localID
		 *            the LocalID
		 * @param p_version
		 *            the version
		 */
		public LIDElement(final long p_localID, final int p_version) {
			m_localID = p_localID;
			m_version = p_version;
		}

		// Getter
		/**
		 * Gets the LocalID
		 * @return the LocalID
		 */
		public long getLocalID() {
			return m_localID;
		}

		/**
		 * Gets the version
		 * @return the version
		 */
		public int getVersion() {
			return m_version;
		}
	}

	/**
	 * Defragments the memory periodical
	 * @author Florian Klein
	 *         05.04.2014
	 */
	private static final class Defragmenter implements Runnable {

		// Constants
		private static final long SLEEP_TIME = 10000;
		private static final double MAX_FRAGMENTATION = 0.75;

		// Attributes
		private boolean m_running;

		// Constructors
		/**
		 * Creates an instance of Defragmenter
		 */
		private Defragmenter() {
			m_running = false;
		}

		// Methods
		/**
		 * Stops the defragmenter
		 */
		private void stop() {
			m_running = false;
		}

		@Override
		public void run() {
			long table;
			int offset;
			double[] fragmentation;

			offset = 0;
			m_running = true;
			while (m_running) {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (final InterruptedException e) {}

				if (m_running) {
					try {
						fragmentation = RawMemory.getFragmentation();

						table = getEntry(offset++, m_nodeIDTableDirectory, 1);
						if (table == 0) {
							offset = 0;
							table = getEntry(offset++, m_nodeIDTableDirectory, 1);
						}

						defragmentTable(table, 1, fragmentation);
					} catch (final MemoryException e) {}
				}
			}
		}

		/**
		 * Defragments a table and its subtables
		 * @param p_addressTable
		 *            the table to defragment
		 * @param p_level
		 *            the level of the table
		 * @param p_fragmentation
		 *            the current fragmentation
		 */
		private void defragmentTable(final long p_addressTable, final int p_level, final double[] p_fragmentation) {
			long entry;
			long address;
			long newAddress;
			int segment;
			byte[] data;

			writeLock(p_addressTable);
			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				try {
					entry = readEntry(p_addressTable, i);
					address = entry & BITMASK_ADDRESS;
					newAddress = 0;

					if (address != 0) {
						segment = RawMemory.getSegment(address);

						if (p_level > 1) {
							defragmentTable(address, p_level - 1, p_fragmentation);

							if (p_fragmentation[segment] > MAX_FRAGMENTATION) {
								data = RawMemory.readBytes(address);
								RawMemory.free(address);
								newAddress = RawMemory.malloc(data.length);
								RawMemory.writeBytes(newAddress, data);
							}
						} else {
							if (p_fragmentation[segment] > MAX_FRAGMENTATION) {
								newAddress = defragmentLevel0Table(address);
							}
						}

						if (newAddress != 0) {
							writeEntry(p_addressTable, i, newAddress + (entry & FULL_FLAG));
						}
					}
				} catch (final MemoryException e) {}
			}
			writeUnlock(p_addressTable);
		}

		/**
		 * Defragments a level 0 table
		 * @param p_addressTable
		 *            the level 0 table to defragment
		 * @return the new table address
		 * @throws MemoryException
		 *             if the table could not be defragmented
		 */
		private long defragmentLevel0Table(final long p_addressTable) throws MemoryException {
			long ret;
			long table;
			long address;
			long[] addresses;
			byte[][] data;
			int[] sizes;
			int position;
			int entries;

			table = p_addressTable;
			entries = ENTRIES_PER_LID_LEVEL + 1;
			addresses = new long[entries];
			Arrays.fill(addresses, 0);
			data = new byte[entries][];
			sizes = new int[entries];
			Arrays.fill(sizes, 0);

			writeLock(table);

			try {
				addresses[0] = table;
				data[0] = RawMemory.readBytes(table);
				sizes[0] = LID_TABLE_SIZE;
				for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
					position = i + 1;

					address = readEntry(table, i) & BITMASK_ADDRESS;
					if (address != 0) {
						addresses[position] = address;
						data[position] = RawMemory.readBytes(address);
						sizes[position] = data[position].length;
					}
				}

				RawMemory.free(addresses);
				addresses = RawMemory.malloc(sizes);

				table = addresses[0];
				RawMemory.writeBytes(table, data[0]);
				for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
					position = i + 1;

					address = addresses[position];
					if (address != 0) {
						writeEntry(table, i, address);

						RawMemory.writeBytes(address, data[position]);
					}
				}
			} finally {
				writeUnlock(table);
			}

			ret = table;

			return ret;
		}

		/**
		 * Defragments all Tables
		 * @return the time of the defragmentation
		 * @throws MemoryException
		 *             if the tables could not be defragmented
		 */
		private long defragmentAll() throws MemoryException {
			long ret;
			double[] fragmentation;

			ret = System.nanoTime();

			fragmentation = RawMemory.getFragmentation();
			defragmentTable(m_nodeIDTableDirectory, LID_TABLE_LEVELS - 1, fragmentation);

			ret = System.nanoTime() - ret;

			return ret;
		}

	}

}
