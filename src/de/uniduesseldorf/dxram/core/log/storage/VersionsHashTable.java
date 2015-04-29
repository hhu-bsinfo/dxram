
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

	private int m_collisions;

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

	/**
	 * Sets the key-value tuple at given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public final void set(final int p_index, final long p_key, final int p_value) {
		int index;

		index = p_index % m_elementCapacity * 3;
		m_table[index] = (int) (p_key >> 32);
		m_table[index + 1] = (int) p_key;
		m_table[index + 2] = p_value;
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index
	 * @return the key
	 */
	public final long getKey(final int p_index) {
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
	public final int getValue(final int p_index) {
		return m_table[p_index % m_elementCapacity * 3 + 2];
	}

	/**
	 * Returns the number of keys in VersionsHashTable
	 * @return the number of keys in VersionsHashTable
	 */
	public final int size() {
		return m_count;
	}

	/**
	 * Tests if VersionsHashTable is empty
	 * @return true if VersionsHashTable maps no keys to values, false otherwise
	 */
	public final boolean isEmpty() {
		return m_count == 0;
	}

	/**
	 * Clears VersionsHashTable
	 */
	public final synchronized void clear() {
		Arrays.fill(m_table, 0);
		m_count = 0;
		m_collisions = 0;
	}

	/**
	 * Returns if there is an entry with given value in VersionsHashTable
	 * @param p_value
	 *            the searched value
	 * @return whether there is an entry with given value in VersionsHashTable or not
	 */
	public final boolean containsValue(final int p_value) {
		boolean ret = false;

		for (int i = 0; i < m_intCapacity && !ret; i++) {
			if (getValue(i) == p_value) {
				ret = true;
				break;
			}
		}
		return ret;
	}

	/**
	 * Returns if there is an entry with given key in VersionsHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return whether there is an entry with given key in VersionsHashTable or not
	 */
	public final boolean containsKey(final long p_key) {
		boolean ret = false;
		int index;
		long iter;
		final long key = p_key + 1;

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
	public final long put(final long p_key, final int p_value) {
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
			} else {
				m_collisions++;
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
	 * Maps the given key to the given value in VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_value
	 *            the value
	 * @return the old value
	 */
	public final long putMax(final long p_key, final int p_value) {
		long ret = -2;
		int index;
		long iter;
		final long key = p_key + 1;

		if ((-1 & 0x0000FFFFFFFFFFFFL) != p_key) {
			index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

			iter = getKey(index);
			while (iter != 0) {
				if (iter == key) {
					ret = getValue(index);
					if (p_value > ret && ret != -1 || p_value == -1) {
						// -1 marks deleted objects
						set(index, key, p_value);
					}
					break;
				} else {
					m_collisions++;
				}
				iter = getKey(++index);
			}
			if (ret == -2) {
				set(index, key, p_value);
				m_count++;
			}

			if (m_count >= m_threshold) {
				rehash();
			}
		}

		return ret;
	}

	/**
	 * Removes the given key from VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @return the value
	 */
	public final long remove(final long p_key) {
		long ret = -1;
		int index;
		long iter;
		final long key = p_key + 1;

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
	 * Increases the capacity of and internally reorganizes VersionsHashTable
	 */
	protected final void rehash() {
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

		System.out.print("Reached threshold (" + oldThreshold
				+ ") -> Rehashing. New size: " + m_elementCapacity + " ... ");

		oldCount = m_count;
		while (index < oldThreshold) {
			if (oldMap[index * 3] != 0) {
				put((long) oldMap[index * 3] << 32 | oldMap[index * 3 + 1] & 0xFFFFFFFFL,
						oldMap[index * 3 + 2] - 1);
			}
			index = (index + 1) % m_elementCapacity;
		}
		m_count = oldCount;
		System.out.println("done");
	}

	/**
	 * Hashes the given key
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	public final int hash(final long p_key) {
		long hash = p_key;

		/*
		 * hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
		 * hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
		 * return (int) ((hash >> 16) ^ hash);
		 */
		hash ^= hash >>> 20 ^ hash >>> 12;
		return (int) (hash ^ hash >>> 7 ^ hash >>> 4);
	}

	/**
	 * Print all tuples in VersionsHashTable
	 */
	public final void printCollisions() {
		System.out.println("Number of collisions: " + m_collisions);
	}

	/**
	 * Print all tuples in VersionsHashTable
	 */
	public final void print() {
		long iter;

		for (int i = 0; i < m_elementCapacity; i++) {
			iter = getKey(i);
			if (iter != 0) {
				System.out.println("Key: " + iter + ", value: " + getValue(i));
			}
		}
	}
}
