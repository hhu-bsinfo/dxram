
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.EpochVersion;
import de.uniduesseldorf.dxram.core.log.LogInterface;

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

	private byte m_eon;
	private short m_epoch;

	private RandomAccessFile m_versionsFile;

	private ReentrantLock m_accessLock;
	private ReentrantLock m_flushLock;

	private LogInterface m_logHandler;

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
		m_threshold = (int) (m_elementCapacity * p_loadFactor);
		m_intCapacity = m_elementCapacity * 4;

		if (m_elementCapacity == 0) {
			m_table = new int[4];
		} else {
			m_table = new int[m_intCapacity];
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

		m_accessLock = new ReentrantLock(false);
		m_flushLock = new ReentrantLock(false);

		try {
			m_logHandler = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			e.printStackTrace();
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
		int count;
		int[] newTable;
		int[] oldTable;
		ByteBuffer buffer;

		// Append all new versions to versions file
		if (m_flushLock.tryLock()) {
			m_accessLock.lock();
			if (m_count > 0) {
				// "Copy" all data to release lock as soon as possible
				count = m_count;
				newTable = new int[m_intCapacity];
				oldTable = m_table;
				m_table = newTable;
				m_count = 0;

				ret = incrementEpoch();
				m_accessLock.unlock();

				try {
					buffer = ByteBuffer.allocate(count * SSD_ENTRY_SIZE);

					// Iterate over all entries (4 bytes per cell)
					for (int i = 0; i < oldTable.length; i += 4) {
						chunkID = (long) oldTable[i] << 32 | (long) oldTable[i + 1] & 0xFFFFFFFFL;
						if (chunkID != 0) {
							// ChunkID (-1 because 1 is added before putting to avoid LID 0)
							buffer.putLong(chunkID - 1);
							// Epoch (4 Bytes in hashtable, 2 in persistent table)
							buffer.putShort((short) oldTable[i + 2]);
							// Version (4 Bytes in hashtable, 3 in persistent table)
							version = oldTable[i + 3];
							buffer.put((byte) (version >>> 16));
							buffer.put((byte) (version >>> 8));
							buffer.put((byte) version);
						}
					}

					m_versionsFile.seek((int) m_versionsFile.length());
					m_versionsFile.write(buffer.array());
				} catch (final IOException e) {
					e.printStackTrace();
				}
			} else {
				m_accessLock.unlock();
			}
			m_flushLock.unlock();
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
		int[] newTable;
		int[] oldTable;
		int[] hashTable;
		ByteBuffer buffer;

		m_flushLock.lock();
		try {
			length = (int) m_versionsFile.length();

			if (length > 0) {
				// Read all entries from SSD to hashtable

				// Read old versions from SSD and add to hashtable
				// Then read all new versions from versions log and add to hashtable (overwrites older entries!)
				data = new byte[length];
				m_versionsFile.seek(0);
				m_versionsFile.readFully(data);
				buffer = ByteBuffer.wrap(data);

				for (int i = 0; i * SSD_ENTRY_SIZE < length; i++) {
					p_allVersions.put(buffer.getLong(), buffer.getShort(), buffer.get() << 16 | buffer.get() << 8 | buffer.get());
				}
			} else {
				// There is nothing on SSD yet
			}

			// Put all versions from versions buffer to VersionsHashTable
			m_accessLock.lock();
			if (m_count > 0) {
				// "Copy" all data to release lock as soon as possible
				newTable = new int[m_intCapacity];
				oldTable = m_table;
				m_table = newTable;
				m_count = 0;

				incrementEpoch();
				m_accessLock.unlock();

				for (int i = 0; i < oldTable.length; i += 4) {
					chunkID = (long) oldTable[i] << 32 | (long) oldTable[i + 1] & 0xFFFFFFFFL;
					if (chunkID != 0) {
						// ChunkID: -1 because 1 is added before putting to avoid LID 0
						p_allVersions.put(chunkID - 1, oldTable[i + 2], oldTable[i + 3]);
					}
				}
			} else {
				m_accessLock.unlock();
			}

			// Write back current hashtable compactified
			data = new byte[p_allVersions.size() * SSD_ENTRY_SIZE];
			buffer = ByteBuffer.wrap(data);

			hashTable = p_allVersions.getTable();
			for (int i = 0; i < hashTable.length; i += 4) {
				chunkID = (long) hashTable[i] << 32 | (long) hashTable[i + 1] & 0xFFFFFFFFL;
				if (chunkID != 0) {
					// ChunkID (-1 because 1 is added before putting to avoid LID 0)
					buffer.putLong(chunkID - 1);
					// Epoch (4 Bytes in hashtable, 2 in persistent table)
					buffer.putShort((short) hashTable[i + 2]);
					// Version (4 Bytes in hashtable, 3 in persistent table)
					version = hashTable[i + 3];
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
		m_flushLock.unlock();
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

		m_accessLock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new EpochVersion((short) getEpoch(index), getVersion(index));
				break;
			}
			iter = getKey(++index);
		}
		m_accessLock.unlock();

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

		// Avoid rehashing by waiting
		while (m_count == m_threshold) {
			m_logHandler.grantAccessToWriterThread();
			Thread.yield();
		}

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_accessLock.lock();
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
		m_accessLock.unlock();

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
		// Avoid rehashing by waiting
		while (m_count == m_threshold) {
			m_logHandler.grantAccessToWriterThread();
			Thread.yield();
		}

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

		m_accessLock.lock();
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
		m_accessLock.unlock();
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
		k1 = k1 << 15 | k1 >>> 17;
		k1 *= c2;
		h1 ^= k1;
		h1 = h1 << 13 | h1 >>> 19;
		h1 = h1 * 5 + 0xe6546b64;

		k1 = (int) ((p_key & 0xff00000000L) + (p_key & 0xff0000000000L)
				+ (p_key & 0xff000000000000L) + (p_key & 0xff000000000000L));
		k1 *= c1;
		k1 = k1 << 15 | k1 >>> 17;
		k1 *= c2;
		h1 ^= k1;
		h1 = h1 << 13 | h1 >>> 19;
		h1 = h1 * 5 + 0xe6546b64;

		h1 ^= 8;
		h1 ^= h1 >>> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >>> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >>> 16;

		return h1;
	}

}
