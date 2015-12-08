
package de.uniduesseldorf.dxram.core.lookup.storage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

import de.uniduesseldorf.dxram.core.lookup.LookupHandler;
import de.uniduesseldorf.dxram.core.util.ChunkID;

/**
 * HashMap to store ID-Mappings (based on java.utils.hashmap, standard-chain hash table)
 * @author Kevin Beineke
 *         22.01.2014
 */
public class AIDTable {

	// Attributes
	private transient Entry[] m_table;
	private transient int m_count;
	private int m_threshold;
	private float m_loadFactor;

	// Constructors
	/**
	 * Creates an instance of IDHashTable
	 */
	public AIDTable() {
		this(20, 0.75f);
	}

	/**
	 * Creates an instance of IDHashTable
	 * @param p_initialCapacity
	 *            the initial capacity of IDHashTable
	 */
	public AIDTable(final int p_initialCapacity) {
		this(p_initialCapacity, 0.75f);
	}

	/**
	 * Creates an instance of IDHashTable
	 * @param p_initialCapacity
	 *            the initial capacity of IDHashTable
	 * @param p_loadFactor
	 *            the load factor of IDHashTable
	 */
	public AIDTable(final int p_initialCapacity, final float p_loadFactor) {
		super();

		m_loadFactor = p_loadFactor;
		if (p_initialCapacity == 0) {
			m_table = new Entry[1];
			m_threshold = (int) p_loadFactor;
		} else {
			m_table = new Entry[p_initialCapacity];
			m_threshold = (int) (p_initialCapacity * p_loadFactor);
		}

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
	 * Returns if there is an entry with given value in IDHashTable
	 * @param p_value
	 *            the searched value
	 * @return whether there is an entry with given value in IDHashTable or not
	 */
	public final boolean containsValue(final long p_value) {
		boolean ret = false;
		Entry iter;

		for (int i = m_table.length - 1; i >= 0 && !ret; i--) {
			iter = m_table[i];
			while (iter != null) {
				if (iter.m_value == p_value) {
					ret = true;
					break;
				}
				iter = iter.m_next;
			}
		}
		return ret;
	}

	/**
	 * Returns if there is an entry with given key in IDHashTable
	 * @param p_key
	 *            the searched key
	 * @return whether there is an entry with given key in IDHashTable or not
	 */
	public final boolean containsKey(final int p_key) {
		boolean ret = false;
		int hash;
		Entry iter;

		hash = hash(p_key);
		iter = m_table[(hash & 0x7FFFFFFF) % m_table.length];
		while (iter != null) {
			if (iter.m_key == p_key) {
				ret = true;
				break;
			}
			iter = iter.m_next;
		}
		return ret;
	}

	/**
	 * Returns the value to which the specified key is mapped in IDHashTable
	 * @param p_key
	 *            the searched key
	 * @return the value to which the key is mapped in IDHashTable
	 */
	public final long get(final int p_key) {
		long ret = 0;
		int hash;
		Entry iter;

		hash = hash(p_key);
		iter = m_table[(hash & 0x7FFFFFFF) % m_table.length];
		while (iter != null) {
			if (iter.m_key == p_key) {
				ret = iter.m_value;
				break;
			}
			iter = iter.m_next;
		}
		return ret;
	}

	/**
	 * Maps the given key to the given value in IDHashTable
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 * @return the old value
	 */
	public final long put(final int p_key, final long p_value) {
		long ret = -1;
		int index;
		int hash;
		Entry newEntry;

		hash = hash(p_key);
		index = (hash & 0x7FFFFFFF) % m_table.length;

		for (Entry e = m_table[index]; e != null; e = e.m_next) {
			if (e.m_key == p_key) {
				ret = e.m_value;
				e.m_value = p_value;
				break;
			}
		}
		if (ret == -1) {
			if (m_count >= m_threshold) {
				rehash();
				index = (hash & 0x7FFFFFFF) % m_table.length;
			}
			newEntry = new Entry(p_key, p_value, m_table[index]);
			m_table[index] = newEntry;
			m_count++;
		}
		return ret;
	}

	/**
	 * Removes the given key from IDHashTable
	 * @param p_key
	 *            the key
	 * @return the value
	 */
	public final long remove(final int p_key) {
		long ret = -1;
		int hash;
		int index;
		Entry prev;
		Entry iter;

		hash = hash(p_key);
		index = (hash & 0x7FFFFFFF) % m_table.length;

		prev = null;
		iter = m_table[index];
		while (iter != null) {
			if (iter.m_key == p_key) {
				if (prev != null) {
					prev.m_next = iter.m_next;
				} else {
					m_table[index] = iter.m_next;
				}
				m_count--;
				ret = iter.m_value;
				break;
			}
			prev = iter;
			iter = iter.m_next;
		}
		return ret;
	}

	/**
	 * Removes all entries of given Peer from IDHashTable
	 * @param p_nodeID
	 *            the NodeID
	 */
	public final void remove(final short p_nodeID) {
		Entry prev;
		Entry iter;

		for (int i = 0; i < m_table.length; i++) {
			iter = m_table[i];
			prev = null;
			while (iter != null) {
				if (ChunkID.getCreatorID(iter.m_value) == p_nodeID) {
					if (prev != null) {
						prev.m_next = iter.m_next;
					} else {
						m_table[i] = iter.m_next;
					}
					m_count--;
				}
				prev = iter;
				iter = iter.m_next;
			}
		}
	}

	/**
	 * Increases the capacity of and internally reorganizes IDHashTable
	 */
	protected final void rehash() {
		int index;
		int oldCapacity;
		int newCapacity;
		Entry old;
		Entry newEntry;
		Entry[] oldMap;
		Entry[] newMap;

		oldCapacity = m_table.length;
		oldMap = m_table;
		newCapacity = oldCapacity * 2 + 1;
		newMap = new Entry[newCapacity];

		m_threshold = (int) (newCapacity * m_loadFactor);
		m_table = newMap;

		System.out.print("Rehashing. New size: " + newCapacity + "...");

		while (oldCapacity > 0) {
			old = oldMap[--oldCapacity];
			while (old != null) {
				newEntry = old;
				old = old.m_next;

				index = (hash(newEntry.m_key) & 0x7FFFFFFF) % newCapacity;
				newEntry.m_next = newMap[index];
				newMap[index] = newEntry;
			}
		}
		System.out.println("done");
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
		Entry iter;

		data = ByteBuffer.allocate(size() * 12);

		for (int i = 0; i < m_table.length; i++) {
			iter = m_table[i];
			if (iter != null) {
				data.putInt(iter.m_key);
				data.putLong(iter.m_value);
				while (iter != null) {
					iter = iter.m_next;
					if (iter != null) {
						data.putInt(iter.m_key);
						data.putLong(iter.m_value);
					}
				}
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
	 * @return all data in IDHashTable
	 */
	public final byte[] toArray(final short p_bound1, final short p_bound2, final boolean p_isOnlySuperpeer, final short p_interval) {
		ByteBuffer data;
		Entry iter;

		data = ByteBuffer.allocate(size() * 12);

		for (int i = 0; i < m_table.length; i++) {
			iter = m_table[i];
			if (iter != null) {
				if (p_isOnlySuperpeer || LookupHandler.isNodeInRange(ChunkID.getCreatorID(iter.m_value), p_bound1, p_bound2, p_interval)) {
					data.putInt(iter.m_key);
					data.putLong(iter.m_value);
				}
				while (iter != null) {
					iter = iter.m_next;
					if (iter != null) {
						if (p_isOnlySuperpeer || LookupHandler.isNodeInRange(ChunkID.getCreatorID(iter.m_value), p_bound1, p_bound2, p_interval)) {
							data.putInt(iter.m_key);
							data.putLong(iter.m_value);
						}
					}
				}
			}
		}
		return data.array();
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
		Entry iter;

		for (int i = 0; i < m_table.length; i++) {
			iter = m_table[i];
			if (iter != null) {
				System.out.println("Key: " + iter.m_key + ", value: " + iter.m_value);
				while (iter != null) {
					iter = iter.m_next;
					if (iter != null) {
						System.out.println("\t Key: " + iter.m_key + ", value: " + iter.m_value);
					}
				}
			}
		}
	}

	/**
	 * Print all tuples in IDHashTable sorted
	 */
	public final void printSorted() {
		Entry iter;
		Collection<Entry> list;

		list = new TreeSet<Entry>(new Comparator<Entry>() {
			@Override
			public int compare(final Entry p_a, final Entry p_b) {
				return p_a.m_key - p_b.m_key;
			}
		});

		for (int i = 0; i < m_table.length; i++) {
			iter = m_table[i];
			if (iter != null) {
				list.add(iter);
				while (iter != null) {
					iter = iter.m_next;
					if (iter != null) {
						list.add(iter);
					}
				}
			}
		}
		for (Entry entry : list) {
			System.out.println("Key: " + entry.m_key + ", value: " + entry.m_value);
		}
	}

	/**
	 * Clears IDHashTable
	 */
	public final synchronized void clear() {
		for (int index = m_table.length; --index >= 0;) {
			m_table[index] = null;
		}
		m_count = 0;
	}

	/*
	 * public final int numberOfCollisions() {
	 * int ret = 0;
	 * Entry iter;
	 * for (int i = 0; i < m_table.length; i++) {
	 * iter = m_table[i];
	 * if (iter == null) {
	 * continue;
	 * }
	 * while (true) {
	 * iter = iter.m_next;
	 * if (iter == null) {
	 * break;
	 * } else {
	 * ret++;
	 * }
	 * }
	 * }
	 * return ret;
	 * }
	 */

	/**
	 * A single Entry in IDHashTable
	 */
	private static class Entry {

		// Attributes
		private int m_key;
		private long m_value;
		private Entry m_next;

		// Constructors
		/**
		 * Creates an instance of Entry
		 * @param p_key
		 *            the key
		 * @param p_value
		 *            the value
		 * @param p_next
		 *            a reference to the successor entry
		 */
		protected Entry(final int p_key, final long p_value, final Entry p_next) {
			m_key = p_key;
			m_value = p_value;
			m_next = p_next;
		}
	}

}
