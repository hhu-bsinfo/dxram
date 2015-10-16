
package de.uniduesseldorf.dxram.core.log.storage;

import java.util.Arrays;

/**
 * HashTable to store versions (Linear probing)
 * @author Kevin Beineke
 *         28.11.2014
 */
public class VersionsHashTable {

	// Attributes
	private int[] m_table;
	private int m_count;
	private int m_intCapacity;
	private int m_elementCapacity;
	private int m_threshold;
	private float m_loadFactor;

	// Constructors
	/**
	 * Creates an instance of VersionsHashTable
	 * @param p_initialElementCapacity
	 *            the initial capacity of VersionsHashTable
	 * @param p_loadFactor
	 *            the load factor of VersionsHashTable
	 */
	public VersionsHashTable(final int p_initialElementCapacity, final float p_loadFactor) {
		super();

		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		m_intCapacity = m_elementCapacity * 3;
		m_loadFactor = p_loadFactor;

		if (m_elementCapacity == 0) {
			m_table = new int[3];
			m_threshold = (int) m_loadFactor;
		} else {
			m_table = new int[m_intCapacity];
			m_threshold = (int) (m_elementCapacity * m_loadFactor);
		}
	}

	// Getter
	/**
	 * Returns the number of keys in VersionsHashTable
	 * @return the number of keys in VersionsHashTable
	 */
	public final int size() {
		return m_count;
	}

	/**
	 * Checks if VersionsHashTable is empty
	 * @return true if VersionsHashTable maps no keys to values, false otherwise
	 */
	public final boolean isEmpty() {
		return m_count == 0;
	}

	// Methods
	/**
	 * Returns the value to which the specified key is mapped in VersionsHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return the value to which the key is mapped in VersionsHashTable
	 */
	public final int get(final long p_key) {
		int ret = 0;
		int index;
		long iter;
		final long key = p_key + 1;

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
	 * Maps the given key to the given value in VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_value
	 *            the value
	 * @return the old value
	 */
	public final long putMax(final long p_key, final int p_value) {
		long ret = 0;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = getValue(index);
				if (ret > 0 && (p_value > ret || p_value < 0)) {
					// Both values are positive and the new one is greater -> newer log entry
					// Or current value is positive and new value is negative -> tombstone
					set(index, key, p_value);
				} else if (ret < 0 && p_value < ret) {
					// Both values are negative and the new one is smaller -> newer tombstone
					set(index, key, p_value);
				}
				break;
			}
			iter = getKey(++index);
		}
		if (ret == 0) {
			// Key unknown until now
			set(index, key, p_value);
			m_count++;
		}

		if (m_count >= m_threshold) {
			rehash();
		}

		return ret;
	}

	/**
	 * Clears VersionsHashTable
	 */
	public final void clear() {
		Arrays.fill(m_table, 0);
		m_count = 0;
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index
	 * @return the key
	 */
	private long getKey(final int p_index) {
		int index;

		index = p_index % m_elementCapacity * 3;
		return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
	}

	/**
	 * Gets the value at given index
	 * @param p_index
	 *            the index
	 * @return the value
	 */
	private int getValue(final int p_index) {
		return m_table[p_index % m_elementCapacity * 3 + 2];
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
	private void set(final int p_index, final long p_key, final int p_value) {
		int index;

		index = p_index % m_elementCapacity * 3;
		m_table[index] = (int) (p_key >> 32);
		m_table[index + 1] = (int) p_key;
		m_table[index + 2] = p_value;
	}

	/**
	 * Hashes the given key
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	private int hash(final long p_key) {
		long hash = p_key;

		hash ^= hash >>> 20 ^ hash >>> 12;
		return (int) (hash ^ hash >>> 7 ^ hash >>> 4);
	}

	/**
	 * Maps the given key to the given value in VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_value
	 *            the value
	 * @return the old value
	 */
	private long put(final long p_key, final int p_value) {
		long ret = -1;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = getValue(index);
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
	 * Increases the capacity of and internally reorganizes VersionsHashTable
	 */
	private void rehash() {
		int index = 0;
		int oldCount;
		int oldThreshold;
		int[] oldMap;
		int[] newMap;

		oldThreshold = m_threshold;
		oldMap = m_table;

		m_elementCapacity = m_elementCapacity * 2 + 1;
		m_intCapacity = m_elementCapacity * 3;
		newMap = new int[m_intCapacity];
		m_threshold = (int) (m_elementCapacity * m_loadFactor);
		m_table = newMap;

		System.out.print("Reached threshold (" + oldThreshold + ") -> Rehashing. New size: " + m_elementCapacity + " ... ");

		oldCount = m_count;
		while (index < oldThreshold) {
			if (oldMap[index * 3] != 0) {
				put((long) oldMap[index * 3] << 32 | oldMap[index * 3 + 1] & 0xFFFFFFFFL, oldMap[index * 3 + 2] - 1);
			}
			index = (index + 1) % m_elementCapacity;
		}
		m_count = oldCount;
		System.out.println("done");
	}

}
