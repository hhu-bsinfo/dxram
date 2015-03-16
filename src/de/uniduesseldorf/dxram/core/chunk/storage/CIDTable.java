package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.locks.JNILock;
import de.uniduesseldorf.dxram.utils.locks.SpinLock;

/**
 * Paging-like Tables for the CID-VA mapping
 * @author Florian Klein
 *         13.02.2014
 */
public final class CIDTable {

	// Constants
	public static final byte ENTRY_SIZE = 5;
	public static final byte TABLE_LEVELS = 4;
	private static final byte BITS_PER_LEVEL = 48 / TABLE_LEVELS;
	private static final long LEVEL_BITMASK = (int)Math.pow(2.0, BITS_PER_LEVEL) - 1;
	public static final int ENTRIES_PER_LEVEL = (int)Math.pow(2.0, BITS_PER_LEVEL);
	public static final int TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LEVEL + 7;
	public static final int TABLE_OFFSET = (int)Math.ceil(Math.log(TABLE_SIZE) / Math.log(1 << 8));
	private static final int LOCK_OFFSET = TABLE_SIZE - 4;

	private static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;
	private static final long BIT_FLAG = 0x8000000000L;
	private static final long FULL_FLAG = BIT_FLAG;
	private static final long DELETED_FLAG = BIT_FLAG;

	// Attributes
	private static long m_tableDirectory;
	private static long m_memoryBase;

	private static CIDStore m_store;

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
		long table;
		long child;

		m_memoryBase = RawMemory.getMemoryBase();
		m_tableDirectory = createTable();

		// Create for every table level one table
		table = m_tableDirectory;
		for (int i = 0;i < TABLE_LEVELS - 1;i++) {
			child = createTable();

			writeEntry(table, 0, child);

			table = child;
		}

		m_store = new CIDStore();

		m_defragmenter = new Defragmenter();
		// TODO: new Thread(m_defragmenter).start();

		System.out.println("CIDTable: init success (page directory at: 0x" + Long.toHexString(m_tableDirectory)
				+ ")");
	}

	/**
	 * Disengages the CIDTable
	 * @throws MemoryException
	 *             if the CIDTable could not be disengaged
	 */
	public static void disengage() throws MemoryException {
		m_defragmenter.stop();
		m_defragmenter = null;

		m_store = null;

		disengage(m_tableDirectory, TABLE_LEVELS - 1);

		m_tableDirectory = 0;
	}

	/**
	 * Disengages a table
	 * @param p_table
	 *            the table
	 * @param p_level
	 *            the table level
	 * @throws MemoryException
	 *             if the table could not be disengaged
	 */
	private static void disengage(final long p_table, final int p_level) throws MemoryException {
		long entry;

		for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
			entry = readEntry(p_table, i) & BITMASK_ADDRESS;

			if (entry > 0) {
				if (p_level > 0) {
					disengage(entry, p_level - 1);
				}
				RawMemory.free(entry);
			}
		}
	}

	/**
	 * Creates a table
	 * @return the address of the table
	 * @throws MemoryException
	 *             if the table could not be created
	 */
	private static long createTable() throws MemoryException {
		long ret;

		ret = RawMemory.malloc(TABLE_SIZE);
		RawMemory.set(ret + TABLE_OFFSET, TABLE_SIZE, (byte)0);

		return ret;
	}

	/**
	 * Reads a table entry
	 * @param p_table
	 *            the table
	 * @param p_index
	 *            the index of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be read
	 */
	private static long readEntry(final long p_table, final long p_index) throws MemoryException {
		return RawMemory.readLong(p_table + ENTRY_SIZE * p_index + TABLE_OFFSET) & 0xFFFFFFFFFFL;
	}

	/**
	 * Writes a table entry
	 * @param p_table
	 *            the table
	 * @param p_index
	 *            the index of the entry
	 * @param p_entry
	 *            the entry
	 * @throws MemoryException
	 *             if the entry could not be written
	 */
	private static void writeEntry(final long p_table, final long p_index, final long p_entry)
			throws MemoryException {
		long value;

		value = RawMemory.readLong(p_table + ENTRY_SIZE * p_index + TABLE_OFFSET) & 0xFFFFFF0000000000L;
		value += p_entry & 0xFFFFFFFFFFL;

		RawMemory.writeLong(p_table + ENTRY_SIZE * p_index + TABLE_OFFSET, value);
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	protected static long get(final long p_cid) throws MemoryException {
		long ret;

		readLock(m_tableDirectory);

		ret = getEntry(p_cid, m_tableDirectory, TABLE_LEVELS - 1);

		readUnlock(m_tableDirectory);

		return ret;
	}

	/**
	 * Gets an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	private static long getEntry(final long p_cid, final long p_table, final int p_level) throws MemoryException {
		long ret = 0;
		long index;
		long entry;

		// readLock(p_table);

		index = p_cid >> BITS_PER_LEVEL * p_level & LEVEL_BITMASK;
		entry = readEntry(p_table, index) & BITMASK_ADDRESS;
		if (p_level > 0) {
			if (entry > 0) {
				ret = getEntry(p_cid, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			ret = entry;
		}

		// readUnlock(p_table);

		return ret;
	}

	/**
	 * Sets an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @param p_address
	 *            the address
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	public static void set(final long p_cid, final long p_address) throws MemoryException {
		setEntry(p_cid, p_address, m_tableDirectory, TABLE_LEVELS - 1);
	}

	/**
	 * Sets an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @param p_address
	 *            the address
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	private static void setEntry(final long p_cid, final long p_address, final long p_table, final int p_level)
			throws MemoryException {
		long index;
		long entry;

		index = p_cid >> BITS_PER_LEVEL * p_level & LEVEL_BITMASK;
		if (p_level > 0) {
			writeLock(p_table);

			// Read table entry
			entry = readEntry(p_table, index);
			if (entry == 0) {
				entry = createTable();
				writeEntry(p_table, index, entry);
			}

			writeUnlock(p_table);

			if (entry > 0) {
				// Set entry in the following table
				setEntry(p_cid, p_address, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			writeLock(p_table);

			// Set the level 0 entry
			writeEntry(p_table, index, p_address & BITMASK_ADDRESS);

			writeUnlock(p_table);
		}
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be get
	 */
	protected static long delete(final long p_cid) throws MemoryException {
		long ret;

		ret = deleteEntry(p_cid, m_tableDirectory, TABLE_LEVELS - 1);

		m_store.put(p_cid);

		return ret;
	}

	/**
	 * Gets and deletes an entry of the level 0 table
	 * @param p_cid
	 *            the cid of the entry
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the table level
	 * @return the entry
	 * @throws MemoryException
	 *             if the entry could not be deleted
	 */
	private static long deleteEntry(final long p_cid, final long p_table, final int p_level) throws MemoryException {
		long ret = -1;
		long index;
		long entry;

		writeLock(p_table);

		index = p_cid >> BITS_PER_LEVEL * p_level & LEVEL_BITMASK;
		if (p_level > 0) {
			// Read table entry
			entry = readEntry(p_table, index);
			if ((entry & FULL_FLAG) > 0) {
				// Delete full flag
				entry &= ~FULL_FLAG;
				writeEntry(p_table, index, entry);
			}

			if ((entry & BITMASK_ADDRESS) > 0) {
				// Delete entry in the following table
				ret = deleteEntry(p_cid, entry & BITMASK_ADDRESS, p_level - 1);
			}
		} else {
			// Read the level 0 entry
			ret = readEntry(p_table, index) & BITMASK_ADDRESS;
			// Delete the level 0 entry
			writeEntry(p_table, index, DELETED_FLAG);
		}

		writeUnlock(p_table);

		return ret;
	}

	/**
	 * Get a free CID from the CIDTable
	 * @return a free CID, or -1 if there is none
	 * @throws MemoryException
	 *             if the CIDTable could not be accessed
	 */
	protected static long getFreeCID() throws MemoryException {
		long ret = -1;

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
		JNILock.readLock(m_memoryBase + p_address + LOCK_OFFSET);
	}

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void readUnlock(final long p_address) {
		JNILock.readUnlock(m_memoryBase + p_address + LOCK_OFFSET);
	}

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void writeLock(final long p_address) {
		JNILock.writeLock(m_memoryBase + p_address + LOCK_OFFSET);
	}

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	private static void writeUnlock(final long p_address) {
		JNILock.writeUnlock(m_memoryBase + p_address + LOCK_OFFSET);
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

		count = new int[TABLE_LEVELS];

		countTables(m_tableDirectory, TABLE_LEVELS - 1, count);

		infos = new StringBuilder();
		infos.append("\nCIDTable:\n");
		for (int i = TABLE_LEVELS - 1;i >= 0;i--) {
			infos.append("\t" + count[i] + " table(s) on level " + i + "\n");
		}

		System.out.println(infos);
	}

	/**
	 * Counts the subtables
	 * @param p_table
	 *            the current table
	 * @param p_level
	 *            the level of the table
	 * @param p_count
	 *            the table counts
	 */
	private static void countTables(final long p_table, final int p_level, final int[] p_count) {
		long entry;

		p_count[p_level]++;

		for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
			try {
				entry = readEntry(p_table, i) & BITMASK_ADDRESS;
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

	// Classes
	/**
	 * Stores free CIDs
	 * @author Florian Klein
	 *         30.04.2014
	 */
	private static final class CIDStore {

		// Constants
		private static final int STORE_CAPACITY = 100;

		// Attributes
		private final long[] m_cids;
		private int m_position;
		private int m_count;
		private volatile long m_overallCount;

		private Lock m_lock;

		// Constructors
		/**
		 * Creates an instance of CIDStore
		 */
		private CIDStore() {
			m_cids = new long[STORE_CAPACITY];
			m_position = 0;
			m_count = 0;

			m_overallCount = 0;

			m_lock = new SpinLock();
		}

		// Methods
		/**
		 * Gets a free CID
		 * @return a free CID
		 */
		public long get() {
			long ret = -1;

			if (m_overallCount > 0) {
				m_lock.lock();

				if (m_count == 0) {
					fill();
				}

				if (m_count > 0) {
					ret = m_cids[m_position];

					m_position = (m_position + 1) % m_cids.length;
					m_count--;
					m_overallCount--;
				}

				m_lock.unlock();
			}

			return ret;
		}

		/**
		 * Puts a free CID
		 * @param p_cid
		 *            a CID
		 */
		public void put(final long p_cid) {
			m_lock.lock();

			if (m_count < m_cids.length) {
				m_cids[m_position + m_count] = p_cid;

				m_count++;
			}
			m_overallCount++;

			m_lock.unlock();
		}

		/**
		 * Fills the store
		 */
		private void fill() {
			try {
				findFreeCIDs();
			} catch (final MemoryException e) {}
		}

		/**
		 * Finds free CIDs in the CIDTable
		 * @throws MemoryException
		 *             if the CIDTable could not be accessed
		 */
		private void findFreeCIDs() throws MemoryException {
			findFreeCIDs(m_tableDirectory, TABLE_LEVELS - 1, 0);
		}

		/**
		 * Finds free CIDs in the CIDTable
		 * @param p_table
		 *            the table
		 * @param p_level
		 *            the table level
		 * @param p_offset
		 *            the offset of the CID
		 * @return true if free CIDs were found, false otherwise
		 * @throws MemoryException
		 *             if the CIDTable could not be accessed
		 */
		private boolean findFreeCIDs(final long p_table, final int p_level, final long p_offset)
				throws MemoryException {
			boolean ret = false;
			long entry;

			writeLock(p_table);

			for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
				// Read table entry
				entry = readEntry(p_table, i);

				if (p_level > 0) {
					if (entry > 0) {
						// Get free CID in the next table
						if (!findFreeCIDs(entry & BITMASK_ADDRESS, p_level - 1, i << BITS_PER_LEVEL * p_level)) {
							// Mark the table as full
							entry |= FULL_FLAG;
							writeEntry(p_table, i, entry);
						} else {
							ret = true;
						}
					}
				} else {
					if ((entry & DELETED_FLAG) > 0) {
						writeEntry(p_table, i, 0);

						m_cids[m_position + m_count] = p_offset + i;
						m_count++;

						ret = true;
					}
				}

				if (m_count == m_cids.length || m_count == m_overallCount) {
					break;
				}
			}

			writeUnlock(p_table);

			return ret;
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

						table = getEntry(offset++, m_tableDirectory, 1);
						if (table == 0) {
							offset = 0;
							table = getEntry(offset++, m_tableDirectory, 1);
						}

						defragmentTable(table, 1, fragmentation);
					} catch (final MemoryException e) {}
				}
			}
		}

		/**
		 * Defragments a table and its subtables
		 * @param p_table
		 *            the table to defragment
		 * @param p_level
		 *            the level of the table
		 * @param p_fragmentation
		 *            the current fragmentation
		 */
		private void defragmentTable(final long p_table, final int p_level, final double[] p_fragmentation) {
			long entry;
			long address;
			long newAddress;
			int segment;
			byte[] data;

			writeLock(p_table);
			for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
				try {
					entry = readEntry(p_table, i);
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
							writeEntry(p_table, i, newAddress + (entry & FULL_FLAG));
						}
					}
				} catch (final MemoryException e) {}
			}
			writeUnlock(p_table);
		}

		/**
		 * Defragments a level 0 table
		 * @param p_table
		 *            the level 0 table to defragment
		 * @return the new table address
		 * @throws MemoryException
		 *             if the table could not be defragmented
		 */
		private long defragmentLevel0Table(final long p_table) throws MemoryException {
			long ret;
			long table;
			long address;
			long[] addresses;
			byte[][] data;
			int[] sizes;
			int position;

			table = p_table;

			addresses = new long[ENTRIES_PER_LEVEL + 1];
			Arrays.fill(addresses, 0);
			data = new byte[ENTRIES_PER_LEVEL + 1][];
			sizes = new int[ENTRIES_PER_LEVEL + 1];
			Arrays.fill(sizes, 0);

			writeLock(table);

			try {
				addresses[0] = table;
				data[0] = RawMemory.readBytes(table);
				sizes[0] = TABLE_SIZE;
				for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
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
				for (int i = 0;i < ENTRIES_PER_LEVEL;i++) {
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
			defragmentTable(m_tableDirectory, TABLE_LEVELS - 1, fragmentation);

			ret = System.nanoTime() - ret;

			return ret;
		}

	}

}
