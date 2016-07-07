
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.utils.CRC16;
import de.hhu.bsinfo.utils.Pair;

/**
 * HashTable to store ID-Mappings (Linear probing)
 * @author Kevin Beineke
 *         27.01.2014
 */
public class NameserviceHashTable {

	// Attributes
	private int[] m_table;
	private int m_count;
	private int m_elementCapacity;
	private int m_threshold;
	private float m_loadFactor;
	private LoggerComponent m_logger;

	// Constructors
	/**
	 * Creates an instance of IDHashTable
	 * @param p_initialElementCapacity
	 *            the initial capacity of IDHashTable
	 * @param p_loadFactor
	 *            the load factor of IDHashTable
	 * @param p_logger
	 *            the LoggerComponent
	 */
	public NameserviceHashTable(final int p_initialElementCapacity, final float p_loadFactor,
			final LoggerComponent p_logger) {
		super();

		m_logger = p_logger;

		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		m_loadFactor = p_loadFactor;

		if (m_elementCapacity == 0) {
			m_table = new int[3];
			m_threshold = (int) m_loadFactor;
		} else {
			m_table = new int[m_elementCapacity * 3];
			m_threshold = (int) (m_elementCapacity * m_loadFactor);
		}
	}

	/**
	 * Sets the key-value tuple at given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public final void set(final int p_index, final int p_key, final long p_value) {
		int index;

		index = p_index % m_elementCapacity * 3;
		m_table[index] = p_key;
		m_table[index + 1] = (int) (p_value >> 32);
		m_table[index + 2] = (int) p_value;
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index
	 * @return the key
	 */
	public final int getKey(final int p_index) {
		return m_table[p_index % m_elementCapacity * 3];
	}

	/**
	 * Gets the value at given index
	 * @param p_index
	 *            the index
	 * @return the value
	 */
	public final long getValue(final int p_index) {
		int index;

		index = p_index % m_elementCapacity * 3 + 1;
		return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
	}

	/**
	 * Returns the number of keys in IDHashTable
	 * @return the number of keys in IDHashTable
	 */
	public final int size() {
		return m_count;
	}

	/**
	 * Tests if IDHashTable is empty
	 * @return true if IDHashTable maps no keys to values, false otherwise
	 */
	public final boolean isEmpty() {
		return m_count == 0;
	}

	/**
	 * Clears IDHashTable
	 */
	public final synchronized void clear() {
		for (int i = 0; i < m_table.length; i++) {
			m_table[i] = 0;
		}
		m_count = 0;
	}

	/**
	 * Returns if there is an entry with given value in IDHashTable
	 * @param p_value
	 *            the searched value
	 * @return whether there is an entry with given value in IDHashTable or not
	 */
	public final boolean containsValue(final long p_value) {
		boolean ret = false;

		for (int i = 0; i < m_elementCapacity * 3 && !ret; i++) {
			if (getValue(i) == p_value) {
				ret = true;
				break;
			}
		}
		return ret;
	}

	/**
	 * Returns if there is an entry with given key in IDHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return whether there is an entry with given key in IDHashTable or not
	 */
	public final boolean containsKey(final int p_key) {
		boolean ret = false;
		int index;
		int iter;
		final int key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = true;
				break;
			}
			iter = getKey(++index);
		}
		return ret;
	}

	/**
	 * Returns the value to which the specified key is mapped in IDHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return the value to which the key is mapped in IDHashTable
	 */
	public final long get(final int p_key) {
		long ret = 0;
		int index;
		int iter;
		final int key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = getValue(index);
				break;
			}
			iter = getKey(++index);
		}

		return ret;
	}

	/**
	 * Get all entries of the nameservice map.
	 * @return Array list with entries as pairs of index + value
	 */
	public ArrayList<Pair<Integer, Long>> get() {
		ArrayList<Pair<Integer, Long>> entries = new ArrayList<Pair<Integer, Long>>(m_count);
		int iter;

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				// rebase index: decrement by one (refer to insert calls)
				entries.add(new Pair<Integer, Long>(iter - 1, getValue(i)));
			}
		}
		return entries;
	}

	/**
	 * Maps the given key to the given value in IDHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_value
	 *            the value
	 * @return the old value
	 */
	public final long put(final int p_key, final long p_value) {
		long ret = -1;
		int index;
		int iter;
		final int key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = getValue(index);
				set(index, key, p_value);
				break;
			}
			iter = getKey(++index);
		}
		if (ret == -1) {
			set(index, key, p_value);
			m_count++;
		}

		if (m_count >= m_threshold) {
			rehash();
		}

		return ret;
	}

	/**
	 * Removes the given key from IDHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @return the value
	 */
	public final long remove(final int p_key) {
		long ret = -1;
		int index;
		int iter;
		final int key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = getValue(index);
				set(index, 0, 0);
				break;
			}
			iter = getKey(++index);
		}

		iter = getKey(++index);
		while (iter != 0) {
			set(index, 0, 0);
			put(iter, getValue(index));

			iter = getKey(++index);
		}

		return ret;
	}

	/**
	 * Removes all entries of given Peer from IDHashTable
	 * @param p_nodeID
	 *            the NodeID
	 */
	public final void remove(final short p_nodeID) {
		int iter;
		int index;

		for (int i = 0; i < m_elementCapacity; i++) {
			if (ChunkID.getCreatorID(getValue(i)) == p_nodeID) {
				set(i, 0, 0);
				m_count--;

				index = i + 1;
				iter = getKey(index);
				while (iter != 0) {
					set(index, 0, 0);
					if (iter == p_nodeID) {
						m_count--;
					} else {
						put(iter, getValue(index));
					}

					iter = getKey(++index);
				}
			}
		}
	}

	/**
	 * Increases the capacity of and internally reorganizes IDHashTable
	 */
	protected final void rehash() {
		int index = 0;
		int oldCount;
		int oldElementCapacity;
		int oldThreshold;
		int[] oldTable;
		int[] newTable;

		oldCount = m_count;
		oldElementCapacity = m_elementCapacity;
		oldThreshold = m_threshold;
		oldTable = m_table;

		m_elementCapacity = m_elementCapacity * 2 + 1;
		newTable = new int[m_elementCapacity * 3];
		m_threshold = (int) (m_elementCapacity * m_loadFactor);
		m_table = newTable;

		// #if LOGGER == TRACE
		// // // // m_logger.trace(getClass(),
				// // // // "Reached threshold (" + oldThreshold + ") -> Rehashing. New size: " + m_elementCapacity + " ... ");
		// #endif /* LOGGER == TRACE */

		m_count = 0;
		while (index < oldElementCapacity) {
			if (oldTable[index * 3] != 0) {
				put(oldTable[index * 3] - 1,
						(long) oldTable[index * 3 + 1] << 32 | oldTable[index * 3 + 2] & 0xFFFFFFFFL);
			}
			index = (index + 1) % m_elementCapacity;
		}
		m_count = oldCount;
		// #if LOGGER == TRACE
		// // // // m_logger.trace(getClass(), "done");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Hashes the given key
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	public final int hash(final int p_key) {
		int hash = p_key;

		hash = (hash >> 16 ^ hash) * 0x45d9f3b;
		hash = (hash >> 16 ^ hash) * 0x45d9f3b;
		return hash >> 16 ^ hash;
		/*
		 * hash ^= (hash >>> 20) ^ (hash >>> 12);
		 * return hash ^ (hash >>> 7) ^ (hash >>> 4);
		 */
	}

	/**
	 * Returns all entries of IDHashTable in a byte array
	 * @return all data in IDHashTable
	 */
	public final byte[] toArray() {
		ByteBuffer data;
		int iter;

		data = ByteBuffer.allocate(m_count * 12);

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				data.putInt(iter);
				data.putLong(getValue(i));
			}
		}
		return data.array();
	}

	/**
	 * Returns all entries of IDHashTable in a byte array
	 * @param p_bound1
	 *            the first bound
	 * @param p_bound2
	 *            the second bound
	 * @param p_isOnlySuperpeer
	 *            whether this is the only superpeer or not
	 * @param p_interval
	 *            the type of interval
	 * @param p_hashGenerator
	 *            the CRC16 hash generator
	 * @return all data in IDHashTable
	 */
	public final byte[] toArray(final short p_bound1, final short p_bound2, final boolean p_isOnlySuperpeer,
			final short p_interval, final CRC16 p_hashGenerator) {
		int count = 0;
		int iter;
		ByteBuffer data;

		data = ByteBuffer.allocate(m_count * 12);

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				if (p_isOnlySuperpeer || OverlayHelper.isNodeInRange(p_hashGenerator.hash(iter - 1), p_bound1, p_bound2,
						p_interval)) {
					data.putInt(iter - 1);
					data.putLong(getValue(i));
					count++;
				}
			}
		}
		return Arrays.copyOfRange(data.array(), 0, count);
	}

	/**
	 * Returns the number of entries without backups
	 * @param p_bound1
	 *            the first bound
	 * @param p_bound2
	 *            the second bound
	 * @param p_isOnlySuperpeer
	 *            whether this is the only superpeer or not
	 * @param p_interval
	 *            the type of interval
	 * @param p_hashGenerator
	 *            the CRC16 hash generator
	 * @return number of own entries
	 */
	public final int getNumberOfOwnEntries(final short p_bound1, final short p_bound2, final boolean p_isOnlySuperpeer,
			final short p_interval, final CRC16 p_hashGenerator) {
		int count = 0;
		int iter;

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				if (p_isOnlySuperpeer || OverlayHelper.isNodeInRange(p_hashGenerator.hash(iter - 1), p_bound1, p_bound2,
						p_interval)) {
					count++;
				}
			}
		}
		return count;
	}

	/**
	 * Puts all mappings stored in a byte array in IDHashTable
	 * @param p_data
	 *            the byte array
	 */
	public final void putAll(final byte[] p_data) {
		ByteBuffer data;

		if (p_data != null) {
			data = ByteBuffer.wrap(p_data);

			for (int i = 0; i < data.capacity() / 12; i++) {
				put(data.getInt(), data.getLong());
			}
		}
	}

	/**
	 * Print all tuples in IDHashTable
	 */
	public final void print() {
		int iter;

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				System.out.println("Key: " + iter + ", value: " + ChunkID.toHexString(getValue(i)));
			}
		}
	}

	/**
	 * Print all tuples in IDHashTable sorted
	 */
	public final void printSorted() {
		int iter;
		Collection<Entry> list;

		list = new TreeSet<Entry>(new Comparator<Entry>() {

			@Override
			public int compare(final Entry p_entryA, final Entry p_entryB) {
				return p_entryA.m_key - p_entryB.m_key;
			}

		});

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				list.add(new Entry(iter, getValue(i)));
			}
		}

		for (Entry entry : list) {
			System.out.println("Key: " + entry.m_key + ", value: " + ChunkID.toHexString(entry.m_value));
		}
	}

	/**
	 * A single Entry in IDHashTable
	 */
	private static class Entry {

		// Attributes
		private int m_key;
		private long m_value;

		// Constructors
		/**
		 * Creates an instance of Entry
		 * @param p_key
		 *            the key
		 * @param p_value
		 *            the value
		 */
		protected Entry(final int p_key, final long p_value) {
			m_key = p_key;
			m_value = p_value;
		}
	}
}
