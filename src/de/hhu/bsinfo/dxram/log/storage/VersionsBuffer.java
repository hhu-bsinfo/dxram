
package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * HashTable to store versions (Linear probing)
 * @author Kevin Beineke
 *         28.11.2014
 */
public class VersionsBuffer {

	// Constants
	private static final int SSD_ENTRY_SIZE = 13;

	// Attributes
	private LogService m_logService;
	private LoggerComponent m_logger;

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

	// Constructors
	/**
	 * Creates an instance of VersionsHashTable
	 * @param p_logService
	 *            the log service to enable calling access granting methods
	 * @param p_logger
	 *            the logger component
	 * @param p_initialElementCapacity
	 *            the initial capacity of VersionsHashTable
	 * @param p_loadFactor
	 *            the load factor of VersionsHashTable
	 * @param p_path
	 *            the versions file's path
	 */
	protected VersionsBuffer(final LogService p_logService, final LoggerComponent p_logger, final int p_initialElementCapacity, final float p_loadFactor,
			final String p_path) {
		super();

		m_logService = p_logService;
		m_logger = p_logger;
		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		m_threshold = (int) (m_elementCapacity * p_loadFactor);
		m_intCapacity = m_elementCapacity * 3;

		if (m_elementCapacity == 0) {
			m_table = new int[3];
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
			m_logger.error(VersionsBuffer.class, "Could not create versions file: " + e);
		}

		m_accessLock = new ReentrantLock(false);
		m_flushLock = new ReentrantLock(false);
	}

	// Getter
	/**
	 * Returns the number of keys in VersionsHashTable
	 * @return the number of keys in VersionsHashTable
	 */
	protected final int getEntryCount() {
		return m_count;
	}

	/**
	 * Returns the number of keys in VersionsHashTable
	 * @return the number of keys in VersionsHashTable
	 */
	protected final long getFileSize() {
		try {
			return m_versionsFile.length();
		} catch (final IOException e) {
			m_logger.error(VersionsBuffer.class, "Could not read versions file's size: " + e);
			return -1;
		}
	}

	/**
	 * Returns the current epoch
	 * @return the current epoch
	 */
	protected final short getEpoch() {
		return m_epoch;
	}

	/**
	 * Returns the current eon
	 * @return the current eon
	 */
	protected final byte getEon() {
		return m_eon;
	}

	/**
	 * Write all versions to SSD and clear hash table
	 * @return whether an overflow during incrementation of epoch occurred or not
	 */
	protected final boolean flush() {
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
					for (int i = 0; i < oldTable.length; i += 3) {
						chunkID = (long) oldTable[i] << 32 | oldTable[i + 1] & 0xFFFFFFFFL;
						if (chunkID != 0) {
							// ChunkID (-1 because 1 is added before putting to avoid LID 0)
							buffer.putLong(chunkID - 1);
							// Epoch (4 Bytes in hashtable, 2 in persistent table; was incremented before!)
							buffer.putShort((short) (m_epoch - 1 + (m_eon << 15)));
							// Version (4 Bytes in hashtable, 3 in persistent table)
							version = oldTable[i + 2];
							buffer.put((byte) (version >>> 16));
							buffer.put((byte) (version >>> 8));
							buffer.put((byte) version);
						}
					}

					m_versionsFile.seek((int) m_versionsFile.length());
					m_versionsFile.write(buffer.array());
				} catch (final IOException e) {
					m_logger.error(VersionsBuffer.class, "Could write to versions file: " + e);
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
	protected final void readAll(final VersionsHashTable p_allVersions) {
		int length;
		int version;
		boolean update = false;
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

				if (p_allVersions.size() * SSD_ENTRY_SIZE < length) {
					// Versions log was not empty -> compact
					update = true;
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

				for (int i = 0; i < oldTable.length; i += 3) {
					chunkID = (long) oldTable[i] << 32 | oldTable[i + 1] & 0xFFFFFFFFL;
					if (chunkID != 0) {
						// ChunkID: -1 because 1 is added before putting to avoid LID 0; epoch was incremented before
						p_allVersions.put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2]);
					}
				}
				update = true;
			} else {
				m_accessLock.unlock();
			}

			if (update) {
				// Write back current hashtable compactified
				data = new byte[p_allVersions.size() * SSD_ENTRY_SIZE];
				buffer = ByteBuffer.wrap(data);

				hashTable = p_allVersions.getTable();
				for (int i = 0; i < hashTable.length; i += 4) {
					chunkID = (long) hashTable[i] << 32 | hashTable[i + 1] & 0xFFFFFFFFL;
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
			}
		} catch (final IOException e) {
			m_logger.error(VersionsBuffer.class, "Could write to versions file: " + e);
		}
		m_flushLock.unlock();
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
	protected final Version get(final long p_key) {
		Version ret = null;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_accessLock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new Version((short) (m_epoch + (m_eon << 15)), getVersion(index));
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
	protected final Version getNext(final long p_key) {
		Version ret = null;
		int index;
		long iter;
		final long key = p_key + 1;

		// Avoid rehashing by waiting
		while (m_count == m_threshold) {
			m_logService.grantAccessToWriterThread();
			Thread.yield();
		}

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_accessLock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new Version((short) (m_epoch + (m_eon << 15)), getVersion(index) + 1);
				set(index, key, ret.getEpoch(), ret.getVersion());
				break;
			}
			iter = getKey(++index);
		}
		if (iter == 0) {
			// First version for this epoch
			ret = new Version((short) (m_epoch + (m_eon << 15)), 1);
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
	protected final void put(final long p_key, final int p_version) {
		// Avoid rehashing by waiting
		while (m_count == m_threshold) {
			m_logService.grantAccessToWriterThread();
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
	protected void put(final long p_key, final int p_epoch, final int p_version) {
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
	 * Gets the key at given index
	 * @param p_index
	 *            the index in table (-> 3 indices per element)
	 * @return the key
	 */
	private long getKey(final int p_index) {
		int index;

		index = p_index % m_elementCapacity * 3;
		return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
	}

	/**
	 * Gets the version at given index
	 * @param p_index
	 *            the index
	 * @return the version
	 */
	private int getVersion(final int p_index) {
		return m_table[p_index % m_elementCapacity * 3 + 2];
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

		index = p_index % m_elementCapacity * 3;
		m_table[index] = (int) (p_key >> 32);
		m_table[index + 1] = (int) p_key;
		m_table[index + 2] = p_version;
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
