
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.log.EpochVersion;

/**
 * HashTable to store versions (Linear probing)
 * @author Kevin Beineke
 *         28.11.2014
 */
public class VersionsBuffer {

	// Constants
	private static final int SSD_ENTRY_SIZE = 13;

	// Attributes
	private int[] m_table;
	private int m_count;
	private int m_intCapacity;
	private int m_elementCapacity;
	private int m_threshold;
	private float m_loadFactor;

	private byte m_eon;
	private short m_epoch;

	private RandomAccessFile m_versionsFile;

	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Creates an instance of VersionsHashTable
	 * @param p_initialElementCapacity
	 *            the initial capacity of VersionsHashTable
	 * @param p_loadFactor
	 *            the load factor of VersionsHashTable
	 * @param p_path
	 *            the versions file's path
	 */
	public VersionsBuffer(final int p_initialElementCapacity, final float p_loadFactor, final String p_path) {
		super();

		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		m_intCapacity = m_elementCapacity * 4;
		m_loadFactor = p_loadFactor;

		if (m_elementCapacity == 0) {
			m_table = new int[4];
			m_threshold = (int) m_loadFactor;
		} else {
			m_table = new int[m_intCapacity];
			m_threshold = (int) (m_elementCapacity * m_loadFactor);
		}

		m_eon = 0;
		m_epoch = 0;

		try {
			final File file = new File(p_path);
			if (file.exists()) {
				file.delete();
			}
			m_versionsFile = new RandomAccessFile(file, "rw");
		} catch (final FileNotFoundException e) {
			System.out.println("Error: Could not create versions file!");
			e.printStackTrace();
		}

		m_lock = new ReentrantLock(false);
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
	 * Returns the current epoch
	 * @return the current epoch
	 */
	public final short getEpoch() {
		return m_epoch;
	}

	/**
	 * Returns the current eon
	 * @return the current eon
	 */
	public final byte getEon() {
		return m_eon;
	}

	/**
	 * Write all versions to SSD and clear hash table
	 * @return whether an overflow during incrementation of epoch occurred or not
	 */
	public final boolean flush() {
		boolean ret = false;
		long chunkID;
		int version;
		ByteBuffer buffer;

		// Append all new versions to versions file
		m_lock.lock();
		if (m_count > 0) {
			try {
				buffer = ByteBuffer.allocate(m_count * SSD_ENTRY_SIZE);

				// Iterate over all entries (4 bytes per cell)
				for (int i = 0; i * 4 < m_table.length; i++) {
					chunkID = getKey(i);
					if (chunkID != 0) {
						// ChunkID
						buffer.putLong(chunkID - 1);
						// Epoch (4 Bytes in hashtable, 2 in persistent table)
						buffer.putShort((short) getEpoch(i));
						// Version (4 Bytes in hashtable, 3 in persistent table)
						version = getVersion(i);
						buffer.put((byte) (version >>> 16));
						buffer.put((byte) (version >>> 8));
						buffer.put((byte) version);
					}
				}

				// Clear versions buffer and increment epoch
				clear();
				ret = incrementEpoch();
				m_lock.unlock();

				m_versionsFile.seek((int) m_versionsFile.length());
				m_versionsFile.write(buffer.array());
			} catch (final IOException e) {
				e.printStackTrace();
				m_lock.unlock();
			}
		} else {
			m_lock.unlock();
		}

		return ret;
	}

	/**
	 * Read all versions from SSD, add current versions and write back
	 * @param p_allVersions
	 *            the VersionsHashTable to put all current versions in
	 */
	public final void readAll(final VersionsHashTable p_allVersions) {
		int length;
		int version;
		long chunkID;
		byte[] data;
		ByteBuffer buffer;

		m_lock.lock();
		// Put all versions from versions buffer to VersionsHashTable
		for (int i = 0; i < m_elementCapacity; i++) {
			chunkID = getKey(i);
			if (chunkID != 0) {
				p_allVersions.put(chunkID - 1, getEpoch(i), getVersion(i));
			}
		}

		// Clear versions buffer and increment epoch
		clear();
		incrementEpoch();
		m_lock.unlock();

		try {
			length = (int) m_versionsFile.length();

			if (length > 0) {
				// Read all entries from SSD to hashtable

				// Read old versions from SSD and add to hashtable
				// Then read all new versions from versions log and add to hashtable (does not overwrite newer entries!)
				data = new byte[length];
				m_versionsFile.seek(0);
				m_versionsFile.readFully(data);
				buffer = ByteBuffer.wrap(data);

				for (int i = 0; i * SSD_ENTRY_SIZE < length; i++) {
					p_allVersions.put(buffer.getLong(), buffer.getShort(), ((int) buffer.get()) << 16 | ((int) buffer.get()) << 8 | buffer.get());
				}
			} else {
				// There is nothing on SSD yet
			}

			// Write back current hashtable compactified
			data = new byte[p_allVersions.size() * SSD_ENTRY_SIZE];
			buffer = ByteBuffer.wrap(data);

			for (int i = 0; i < p_allVersions.getTableLength(); i++) {
				chunkID = p_allVersions.getKey(i);
				if (chunkID != 0) {
					// ChunkID
					buffer.putLong(chunkID - 1);
					// Epoch (4 Bytes in hashtable, 2 in persistent table)
					buffer.putShort((short) p_allVersions.getEpoch(i));
					// Version (4 Bytes in hashtable, 3 in persistent table)
					version = p_allVersions.getVersion(i);
					buffer.put((byte) (version >>> 16));
					buffer.put((byte) (version >>> 8));
					buffer.put((byte) version);
				}
			}
			m_versionsFile.seek(0);
			m_versionsFile.write(data);
			m_versionsFile.setLength(data.length);

		} catch (final IOException e) {
			e.printStackTrace();
		}
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
	 * Increments the epoch
	 * @return whether an overflow occurred or not
	 */
	private boolean incrementEpoch() {
		boolean ret = false;

		m_epoch++;

		// Overflow
		if (m_epoch == 0) {
			m_eon = (byte) (m_eon ^ 1);
			ret = true;
		}

		return ret;
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

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_lock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new EpochVersion((short) getEpoch(index), getVersion(index));
				break;
			}
			iter = getKey(++index);
		}
		m_lock.unlock();

		return ret;
	}

	/**
	 * Returns the next value to which the specified key is mapped in VersionsHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return the 1 + value to which the key is mapped in VersionsHashTable
	 */
	public final EpochVersion getNext(final long p_key) {
		EpochVersion ret = null;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_lock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new EpochVersion((short) (m_epoch + (m_eon << 15)), getVersion(index) + 1);
				set(index, key, ret.getEpoch(), ret.getVersion());
				break;
			}
			iter = getKey(++index);
		}
		if (iter == 0) {
			// First version for this epoch
			ret = new EpochVersion((short) (m_epoch + (m_eon << 15)), 1);
			set(index, key, ret.getEpoch(), ret.getVersion());
			m_count++;
		}

		if (m_count >= m_threshold) {
			rehash();
		}
		m_lock.unlock();

		return ret;
	}

	/**
	 * Maps the given key to the given value in VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_version
	 *            the version
	 */
	public final void put(final long p_key, final int p_version) {
		put(p_key, (short) (m_epoch + (m_eon << 15)), p_version);
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

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_lock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				set(index, key, p_epoch, p_version);
				break;
			}
			iter = getKey(++index);
		}
		if (iter == 0) {
			// Key unknown until now
			set(index, key, p_epoch, p_version);
			m_count++;
		}

		if (m_count >= m_threshold) {
			rehash();
		}
		m_lock.unlock();
	}

	/**
	 * Clears hashtable
	 */
	public final void clear() {
		Arrays.fill(m_table, 0);
		m_count = 0;
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index in table (-> 4 indices per element)
	 * @return the key
	 */
	private long getKey(final int p_index) {
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
	private int getEpoch(final int p_index) {
		return m_table[p_index % m_elementCapacity * 4 + 2];
	}

	/**
	 * Gets the version at given index
	 * @param p_index
	 *            the index
	 * @return the version
	 */
	private int getVersion(final int p_index) {
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

	/**
	 * Hashes the given key
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	/*-private int hash(final long p_key) {
		long hash = p_key;

		hash = (~hash) + (hash << 18); // hash = (hash << 18) - hash - 1;
		hash = hash ^ (hash >>> 31);
		hash = hash * 21; // hash = (hash + (hash << 2)) + (hash << 4);
		hash = hash ^ (hash >>> 11);
		hash = hash + (hash << 6);
		hash = hash ^ (hash >>> 22);
		return (int) hash;

		hash = (hash >> 16 ^ hash) * 0x45d9f3b;
		hash = (hash >> 16 ^ hash) * 0x45d9f3b;
		return (int) (hash >> 16 ^ hash);
	}*/

	/**
	 * Hashes the given key with MurmurHash3
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	public static int hash(final long p_key) {
		final int c1 = 0xcc9e2d51;
		final int c2 = 0x1b873593;
		int h1 = 0x9747b28c;
		int k1;

		k1 = ((short) p_key & 0xff) + ((int) p_key & 0xff00) + ((int) p_key & 0xff0000) + ((int) p_key & 0xff000000);
		k1 *= c1;
		k1 = (k1 << 15) | (k1 >>> 17);
		k1 *= c2;
		h1 ^= k1;
		h1 = (h1 << 13) | (h1 >>> 19);
		h1 = h1 * 5 + 0xe6546b64;

		k1 = (int) (((long) p_key & 0xff00000000L) + ((long) p_key & 0xff0000000000L)
				+ ((long) p_key & 0xff000000000000L) + ((long) p_key & 0xff000000000000L));
		k1 *= c1;
		k1 = (k1 << 15) | (k1 >>> 17);
		k1 *= c2;
		h1 ^= k1;
		h1 = (h1 << 13) | (h1 >>> 19);
		h1 = h1 * 5 + 0xe6546b64;

		h1 ^= 8;
		h1 ^= h1 >>> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >>> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >>> 16;

		return h1;
	}

	/**
	 * Increases the capacity of and internally reorganizes VersionsHashTable
	 */
	private void rehash() {
		int index = 0;
		int oldCapacity;
		int[] oldMap;
		int[] newMap;

		oldCapacity = m_intCapacity;
		oldMap = m_table;

		m_elementCapacity = m_elementCapacity * 2 + 1;
		m_intCapacity = m_elementCapacity * 4;
		newMap = new int[m_intCapacity];
		m_threshold = (int) (m_elementCapacity * m_loadFactor);
		m_table = newMap;

		m_count = 0;
		while (index < oldCapacity) {
			if (((long) oldMap[index] << 32 | oldMap[index + 1] & 0xFFFFFFFFL) != 0) {
				put(((long) oldMap[index] << 32 | oldMap[index + 1] & 0xFFFFFFFFL) - 1, oldMap[index + 2], oldMap[index + 3]);
			}
			index += 4;
		}
	}

}
