
package de.uniduesseldorf.dxram.core.log.storage;

import java.util.Arrays;

import de.uniduesseldorf.dxram.core.log.EpochVersion;

/**
 * HashTable to store versions (Linear probing)
 * @author Kevin Beineke
 *         28.11.2014
 */
public class VersionsHashTable {

	// Attributes
	private int[] m_table;
	private int m_count;
	private int m_elementCapacity;

	// Constructors
	/**
	 * Creates an instance of VersionsHashTable
	 * @param p_initialElementCapacity
	 *            the initial capacity of VersionsHashTable
	 */
	public VersionsHashTable(final int p_initialElementCapacity) {
		super();

		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		if (p_initialElementCapacity == 0) {
			m_elementCapacity = 100;
		}

		if (m_elementCapacity == 0) {
			m_table = new int[4];
		} else {
			m_table = new int[m_elementCapacity * 4];
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
	 * Returns the number of array cells in VersionsHashTable
	 * @return the number of array cells in VersionsHashTable
	 */
	public final int getTableLength() {
		return m_elementCapacity;
	}

	// Methods
	/**
	 * Clears VersionsHashTable
	 */
	public final void clear() {
		Arrays.fill(m_table, 0);
		m_count = 0;
	}

	/**
	 * Returns the value to which the specified key is mapped in VersionsHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return the value to which the key is mapped in VersionsHashTable
	 */
	public final EpochVersion get(final long p_key) {
		EpochVersion ret = null;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (VersionsBuffer.hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new EpochVersion((short) getEpoch(index), getVersion(index));
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
	 * @param p_epoch
	 *            the epoch
	 * @param p_version
	 *            the version
	 */
	public void put(final long p_key, final int p_epoch, final int p_version) {
		int index;
		long iter;
		final long key = p_key + 1;

		index = (VersionsBuffer.hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				// Do not overwrite entry!
				break;
			}
			iter = getKey(++index);
		}
		if (iter == 0) {
			// Key unknown until now
			set(index, key, p_epoch, p_version);
			m_count++;
		}

		if (m_count == m_elementCapacity) {
			System.out.println("Error: HashTable too small!");
		}
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index
	 * @return the key
	 */
	protected long getKey(final int p_index) {
		int index;

		index = p_index % m_elementCapacity * 4;
		return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
	}

	/**
	 * Gets the epoch at given index
	 * @param p_index
	 *            the index
	 * @return the epoch
	 */
	protected int getEpoch(final int p_index) {
		return m_table[p_index % m_elementCapacity * 4 + 2];
	}

	/**
	 * Gets the version at given index
	 * @param p_index
	 *            the index
	 * @return the version
	 */
	protected int getVersion(final int p_index) {
		return m_table[p_index % m_elementCapacity * 4 + 3];
	}

	/**
	 * Sets the key-value tuple at given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_epoch
	 *            the epoch
	 * @param p_version
	 *            the version
	 */
	private void set(final int p_index, final long p_key, final int p_epoch, final int p_version) {
		int index;

		index = p_index % m_elementCapacity * 4;
		m_table[index] = (int) (p_key >> 32);
		m_table[index + 1] = (int) p_key;
		m_table[index + 2] = p_epoch;
		m_table[index + 3] = p_version;
	}
}
