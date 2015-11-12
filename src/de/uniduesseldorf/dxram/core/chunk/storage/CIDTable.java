
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
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
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
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

	protected static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;
	protected static final long BIT_FLAG = 0x8000000000L;
	protected static final long FULL_FLAG = BIT_FLAG;
	protected static final long DELETED_FLAG = BIT_FLAG;

	// Attributes
	// have these protected for the defragmenter to access
	protected long m_nodeIDTableDirectory;
	protected SmallObjectHeap m_rawMemory;

	private LIDStore m_store;

	private Defragmenter m_defragmenter;

	// Constructors
	/**
	 * Creates an instance of CIDTable
	 */
	public CIDTable() {}

	// Methods
	/**
	 * Initializes the CIDTable
	 * @param p_rawMemory The raw memory instance to use for allocation.
	 * @throws MemoryException
	 *             if the CIDTable could not be initialized
	 */
	public void initialize(final SmallObjectHeap p_rawMemory) throws MemoryException {
		m_rawMemory = p_rawMemory;
		m_nodeIDTableDirectory = createNIDTable();

		m_store = new LIDStore();

		m_defragmenter = new Defragmenter(this);
		// TODO: new Thread(m_defragmenter).start();

		System.out.println("CIDTable: init success (page directory at: 0x" + Long.toHexString(m_nodeIDTableDirectory) + ")");
	}

	/**
	 * Disengages the CIDTable
	 * @throws MemoryException
	 *             if the CIDTable could not be disengaged
	 */
	public void disengage() throws MemoryException {
		long entry;

		m_defragmenter.stop();
		m_defragmenter = null;

		m_store = null;

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
	protected long readEntry(final long p_addressTable, final long p_index) throws MemoryException {
		long ret;

		if (p_addressTable == m_nodeIDTableDirectory) {
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
	 * @throws MemoryException
	 *             if the entry could not be written
	 */
	protected void writeEntry(final long p_addressTable, final long p_index, final long p_entry) throws MemoryException {
		long value;

		value = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFF0000000000L;
		value += p_entry & 0xFFFFFFFFFFL;

		m_rawMemory.writeLong(p_addressTable, ENTRY_SIZE * p_index, value);
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

		ret = getEntry(p_chunkID, m_nodeIDTableDirectory, LID_TABLE_LEVELS);

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
	protected long getEntry(final long p_chunkID, final long p_addressTable, final int p_level) throws MemoryException {
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
			//readLock(p_addressTable);
			entry = readEntry(p_addressTable, index) & BITMASK_ADDRESS;
			//readUnlock(p_addressTable);
		}		

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
			//writeLock(p_addressTable);

			// Read table entry
			entry = readEntry(p_addressTable, index);
			if (entry == 0) {
				entry = createLIDTable();
				writeEntry(p_addressTable, index, entry);
			}

			//writeUnlock(p_addressTable);

			if (entry > 0) {
				// move on to next table
				setEntry(p_chunkID, p_addressChunk, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			//writeLock(p_addressTable);

			// Set the level 0 entry
			// valid and active entry, delete flag 0
			writeEntry(p_addressTable, index, p_addressChunk & BITMASK_ADDRESS);

			//writeUnlock(p_addressTable);
		}
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_flagZombie Flag the deleted entry as a zombie or not zombie i.e. fully deleted.
	 * @return A pair with a bool indicating that the entry was fully removed and memory
	 *         needs to be free'd. If false the entry was flaged as deleted/zombie i.e.
	 *         do not free the memory for it, yet.
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	public long delete(final long p_chunkID, final boolean p_flagZombie) throws MemoryException {
		long ret;
		ret = deleteEntry(p_chunkID, m_nodeIDTableDirectory, LID_TABLE_LEVELS, p_flagZombie);
		return ret;
	}

	/**
	 * Puts the LocalID of a deleted migrated Chunk to LIDStore
	 * @param p_chunkID
	 *            the ChunkID of the entry
	 * @param p_version
	 *            the version of the entry
	 * @return m_cidTable
	 */
	protected boolean putChunkIDForReuse(final long p_chunkID, final int p_version) {
		return m_store.put(ChunkID.getLocalID(p_chunkID), p_version);
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
	 * @throws MemoryException
	 *             if the entry could not be deleted
	 */
	private long deleteEntry(final long p_chunkID, final long p_addressTable, final int p_level, final boolean p_flagZombie) throws MemoryException {
		long ret = -1;
		long index;
		long entry;

		//writeLock(p_addressTable);

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

		//writeUnlock(p_addressTable);

		return ret;
	}

	/**
	 * Returns the ChunkIDs of all migrated Chunks
	 * @return the ChunkIDs of all migrated Chunks
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
		ArrayList<Long> ret = null;
		long entry;

		if (m_store != null) {

			//readLock(m_nodeIDTableDirectory);

			ret = new ArrayList<Long>();
			for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
				entry = readEntry(m_nodeIDTableDirectory, i) & BITMASK_ADDRESS;
				if (entry > 0 && i != (NodeID.getLocalNodeID() & 0xFFFF)) {
					ret.addAll(getAllEntries((long) i << 48, readEntry(m_nodeIDTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1));
				}
			}

			//readUnlock(m_nodeIDTableDirectory);
		}

		return ret;
	}

	/**
	 * Returns the ChunkID ranges of all locally stored Chunks
	 * @return the ChunkID ranges in an ArrayList
	 * @throws MemoryException
	 *             if the CIDTable could not be completely accessed
	 */
	public ArrayList<Long> getCIDrangesOfAllLocalChunks() throws MemoryException {
		ArrayList<Long> ret = null;
		long entry;
		long intervalStart;
		long intervalEnd;

		if (m_store != null) {

			//readLock(m_nodeIDTableDirectory);

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

			//readUnlock(m_nodeIDTableDirectory);
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
	private ArrayList<Long> getAllRanges(final long p_unfinishedCID, final long p_table, final int p_level) throws MemoryException {
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
	private ArrayList<Long> getAllEntries(final long p_unfinishedCID, final long p_table, final int p_level) throws MemoryException {
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
	protected LIDElement getFreeLID() throws MemoryException {
		LIDElement ret = null;

		if (m_store != null) {
			ret = m_store.get();
		}

		return ret;
	}

	/**
	 * Defragments all Tables
	 * @return the time of the defragmentation
	 * @throws MemoryException
	 *             if the tables could not be defragmented
	 */
	public long defragmentAll() throws MemoryException {
		return m_defragmenter.defragmentAll();
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

	// Classes
	/**
	 * Stores free LocalIDs
	 * @author Florian Klein
	 *         30.04.2014
	 */
	private final class LIDStore {

		// Constants
		private static final int STORE_CAPACITY = 100000;

		// Attributes
		private final LIDElement[] m_localIDs;
		private int m_position;
		// available free lid elements stored in our array
		private int m_count;
		// This counts the total available lids in the array
		// as well as elements that are still allocated
		// (because they don't fit into the local array anymore)
		// but not valid -> zombies
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

				if (m_count == 0) {
					fill();
				}

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
		 * @return True if adding an entry to our local ID store was successful, false otherwise.
		 */
		public boolean put(final long p_localID, final int p_version) {
			boolean ret;

			m_lock.lock();

			if (m_count < m_localIDs.length) {
				m_localIDs[m_position + m_count] = new LIDElement(p_localID, p_version);

				m_count++;
				ret = true;
			} else {
				ret = false;
			}

			m_overallCount++;

			m_lock.unlock();

			return ret;
		}

		/**
		 * Fills the store
		 */
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

			//writeLock(p_addressTable);

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
						long chunkAddress;
						long chunkID;
						long nodeID;
						long localID;
						int sizeVersion;
						int version;

						nodeID = NodeID.getLocalNodeID();
						localID = p_offset + i;

						chunkID = nodeID << 48;
						chunkID = chunkID + localID;

						// get zombie chunk that is still allocated
						// to preserve the version data
						chunkAddress = CIDTable.this.get(chunkID);

						sizeVersion = m_rawMemory.getCustomState(chunkAddress) + 1;
						version = CIDTable.this.readVersion(chunkAddress, sizeVersion);

						// cleanup zombie in table
						writeEntry(p_addressTable, i, 0);

						m_localIDs[m_position + m_count] = new LIDElement(localID, version);
						m_count++;

						// cleanup zombie in raw memory
						m_rawMemory.free(entry & BITMASK_ADDRESS);

						ret = true;
					}
				}

				if (m_count == m_localIDs.length || m_count == m_overallCount) {
					break;
				}
			}

			//writeUnlock(p_addressTable);

			return ret;
		}
	}
	
//	/**
//	 * Locks the read lock
//	 * @param p_address
//	 *            the address of the lock
//	 */
//	private void readLock(final long p_address) {
//		if (p_address == m_nodeIDTableDirectory) {
//			m_rawMemory.readLock(p_address + NID_LOCK_OFFSET);
//		} else {
//			m_rawMemory.readLock(p_address + LID_LOCK_OFFSET);
//		}
//	}
//
//	/**
//	 * Unlocks the read lock
//	 * @param p_address
//	 *            the address of the lock
//	 */
//	private void readUnlock(final long p_address) {
//		if (p_address == m_nodeIDTableDirectory) {
//			m_rawMemory.readUnlock(p_address + NID_LOCK_OFFSET);
//		} else {
//			m_rawMemory.readUnlock(p_address + LID_LOCK_OFFSET);
//		}
//	}
//
//	/**
//	 * Locks the write lock
//	 * @param p_address
//	 *            the address of the lock
//	 */
//	private void writeLock(final long p_address) {
//		if (p_address == m_nodeIDTableDirectory) {
//			m_rawMemory.writeLock(p_address + NID_LOCK_OFFSET);
//		} else {
//			m_rawMemory.writeLock(p_address + LID_LOCK_OFFSET);
//		}
//	}
//
//	/**
//	 * Unlocks the write lock
//	 * @param p_address
//	 *            the address of the lock
//	 */
//	private void writeUnlock(final long p_address) {
//		if (p_address == m_nodeIDTableDirectory) {
//			m_rawMemory.writeUnlock(p_address + NID_LOCK_OFFSET);
//		} else {
//			m_rawMemory.writeUnlock(p_address + LID_LOCK_OFFSET);
//		}
//	}

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

}
