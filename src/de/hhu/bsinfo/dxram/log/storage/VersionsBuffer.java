package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.LogComponent;

/**
 * HashTable to store versions (Linear probing)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2016
 */
class VersionsBuffer {

    // Constants
    private static final int VERSIONS_BUFFER_CAPACITY = 262144;
    private static final float FLUSH_FACTOR = 0.65f;
    private static final float LOAD_FACTOR = 0.9f;
    private static final int FLUSH_THRESHOLD = (int) (VERSIONS_BUFFER_CAPACITY * FLUSH_FACTOR);
    private static final int WAIT_THRESHOLD = (int) (VERSIONS_BUFFER_CAPACITY * LOAD_FACTOR);
    private static final int SSD_ENTRY_SIZE = 13;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionsBuffer.class.getSimpleName());

    // Attributes
    private static ByteBuffer ms_flushBuffer;

    private LogComponent m_logComponent;
    private long m_highestChunkID = 0;
    private int[] m_table;
    private int m_count;
    private int m_logCount;
    private int m_intCapacity;
    private byte m_eon;
    private short m_epoch;
    private RandomAccessFile m_versionsFile;

    private ReentrantLock m_accessLock;

    // Constructors

    /**
     * Creates an instance of VersionsBuffer
     *
     * @param p_logComponent
     *     the log component to enable calling access granting methods
     * @param p_path
     *     the versions file's path
     */
    VersionsBuffer(final LogComponent p_logComponent, final String p_path) {
        super();

        m_logComponent = p_logComponent;
        m_count = 0;
        m_logCount = 0;
        m_intCapacity = VERSIONS_BUFFER_CAPACITY * 3;

        m_table = new int[m_intCapacity];
        ms_flushBuffer = ByteBuffer.allocate(SSD_ENTRY_SIZE * VERSIONS_BUFFER_CAPACITY);

        m_eon = 0;
        m_epoch = 0;

        try {
            final File file = new File(p_path);
            if (file.exists()) {
                if (!file.delete()) {
                    throw new FileNotFoundException();
                }
            }
            m_versionsFile = new RandomAccessFile(file, "rw");
        } catch (final FileNotFoundException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not create versions file", e);
            // #endif /* LOGGER >= ERROR */
        }

        m_accessLock = new ReentrantLock(false);
    }

    // Getter

    /**
     * Returns the number of keys in VersionsBuffer
     *
     * @return the number of keys in VersionsBuffer
     */
    final boolean isThresholdReached() {
        return m_count >= FLUSH_THRESHOLD;
    }

    /**
     * Returns the number of keys in VersionsBuffer
     *
     * @return the number of keys in VersionsBuffer
     */
    final long getFileSize() {
        try {
            return m_versionsFile.length();
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not read versions file's size", e);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }
    }

    /**
     * Returns the current epoch
     *
     * @return the current epoch
     */
    final short getEpoch() {
        return m_epoch;
    }

    /**
     * Returns the current eon
     *
     * @return the current eon
     */
    final byte getEon() {
        return m_eon;
    }

    /**
     * Hashes the given key with MurmurHash3
     *
     * @param p_key
     *     the key
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

        k1 = (int) ((p_key & 0xff00000000L) + (p_key & 0xff0000000000L) + (p_key & 0xff000000000000L) + (p_key & 0xff000000000000L));
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

    /**
     * Write all versions to SSD and clear hash table
     *
     * @return whether an overflow during incrementation of epoch occurred or not
     */
    protected final boolean flush() {
        boolean ret = false;
        long chunkID;
        int version;
        int count;
        int[] newTable;
        int[] oldTable;

        // Append all new versions to versions file
        m_accessLock.lock();
        if (m_count > 0) {
            // "Copy" all data to release lock as soon as possible
            count = m_count;
            newTable = new int[m_intCapacity];
            oldTable = m_table;
            m_table = newTable;
            m_logCount += m_count;
            m_count = 0;

            ret = incrementEpoch();
            m_accessLock.unlock();

            try {
                // Re-use ByteBuffer
                Arrays.fill(ms_flushBuffer.array(), count * SSD_ENTRY_SIZE, ms_flushBuffer.capacity() - 1, (byte) 0);
                ms_flushBuffer.clear();

                // Iterate over all entries (4 bytes per cell)
                for (int i = 0; i < oldTable.length; i += 3) {
                    chunkID = (long) oldTable[i] << 32 | oldTable[i + 1] & 0xFFFFFFFFL;
                    if (chunkID != 0) {
                        // ChunkID (-1 because 1 is added before putting to avoid LID 0)
                        ms_flushBuffer.putLong(chunkID - 1);
                        // Epoch (2 Bytes in persistent table; was incremented before!)
                        ms_flushBuffer.putShort((short) (m_epoch - 1 + (m_eon << 15)));
                        // Version (4 Bytes in hashtable, 3 in persistent table)
                        version = oldTable[i + 2];
                        ms_flushBuffer.put((byte) (version >>> 16));
                        ms_flushBuffer.put((byte) (version >>> 8));
                        ms_flushBuffer.put((byte) version);
                    }
                }

                m_versionsFile.seek((int) m_versionsFile.length());
                m_versionsFile.write(ms_flushBuffer.array(), 0, count * SSD_ENTRY_SIZE);
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could write to versions file", e);
                // #endif /* LOGGER >= ERROR */
            }
        } else {
            m_accessLock.unlock();
        }

        return ret;
    }

    /**
     * Returns the value to which the specified key is mapped in VersionsBuffer
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in VersionsBuffer
     */
    protected final Version get(final long p_key) {
        Version ret = null;
        int index;
        long iter;
        final long key = p_key + 1;

        index = (hash(key) & 0x7FFFFFFF) % VERSIONS_BUFFER_CAPACITY;

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
     * Maps the given key to the given value in VersionsBuffer
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_version
     *     the version
     */
    protected final void put(final long p_key, final int p_version) {
        // Avoid rehashing and excessive memory usage by waiting
        while (m_count == WAIT_THRESHOLD) {
            m_logComponent.grantAccessToWriterThread();
            Thread.yield();
        }

        putInternal(p_key, p_version);
    }

    // Methods

    /**
     * Returns the number of entries in this range. Might differ from m_logCount as not every Chunk in range might have been put, already.
     * Only usable without migrations.
     *
     * @param p_offset
     *     the first ChunkID in range
     * @return the number of entries in this range
     */
    final int getRangeSize(final long p_offset) {
        return (int) (ChunkID.getLocalID(m_highestChunkID) - ChunkID.getLocalID(p_offset)) + 1;
    }

    /**
     * Maps the given key to the given value in VersionsBuffer if VersionBuffer is not completely filled
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_version
     *     the version
     */
    final void tryPut(final long p_key, final int p_version) {
        // Avoid rehashing and excessive memory usage by waiting
        if (m_count == WAIT_THRESHOLD) {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not transfer log entry to new eon as current epoch is full");
            // #endif /* LOGGER >= WARN */

            return;
        }

        putInternal(p_key, p_version);
    }

    /**
     * Read all versions from SSD, add current versions and write back
     *
     * @param p_allVersions
     *     the array to put eons, epochs and versions in
     * @param p_offset
     *     the first ChunkID in backup range
     * @return the former epoch
     */
    final short readAll(final TemporaryVersionsStorage p_allVersions, final boolean p_logStoresMigrations, final long p_offset) {
        if (!p_logStoresMigrations) {
            return readAll(p_allVersions, p_offset, true);
        } else {
            return readAllMigrations(p_allVersions, true);
        }
    }

    /**
     * Returns the next value to which the specified key is mapped in VersionsBuffer
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @return the 1 + value to which the key is mapped in VersionsBuffer
     */
    final Version getNext(final long p_key) {
        Version ret = null;
        int index;
        long iter;
        final long key = p_key + 1;

        // Avoid rehashing by waiting
        while (m_count == WAIT_THRESHOLD) {
            m_logComponent.grantAccessToWriterThread();
            Thread.yield();
        }

        index = (hash(key) & 0x7FFFFFFF) % VERSIONS_BUFFER_CAPACITY;

        m_accessLock.lock();
        if (ChunkID.getLocalID(p_key) > m_highestChunkID) {
            m_highestChunkID = p_key;
        }

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                ret = new Version((short) (m_epoch + (m_eon << 15)), getVersion(index) + 1);
                set(index, key, ret.getVersion());
                break;
            }
            iter = getKey(++index);
        }
        if (iter == 0) {
            // First version for this epoch
            ret = new Version((short) (m_epoch + (m_eon << 15)), 1);
            set(index, key, ret.getVersion());
            m_count++;
        }
        m_accessLock.unlock();

        return ret;
    }

    /**
     * Read all versions from SSD, add current versions and write back (if specified, only)
     * Is used for normal secondary logs, as ChunkID range is known.
     *
     * @param p_allVersions
     *     the array to put eons, epochs and versions in
     * @param p_offset
     *     the first ChunkID in backup range
     * @param p_writeBack
     *     whether the versions should be written-back for compactification
     * @return the versions (might have been enlarged)
     */
    private short readAll(final TemporaryVersionsStorage p_allVersions, final long p_offset, final boolean p_writeBack) {
        short ret;
        int length;
        int version;
        boolean update = false;
        long chunkID;
        byte[] data;
        int[] newTable;
        int[] oldTable;
        ByteBuffer buffer;
        int counter = 0;
        int[] versions;

        versions = p_allVersions.getVersionsForNormalLog();
        if (getRangeSize(p_offset) > versions.length / 2) {
            // There are more log entries in this secondary log than in any before -> increase array size
            versions = p_allVersions.resizeVersionsForNormalLog(2 * getRangeSize(p_offset));
        }

        ret = m_epoch;
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
                    chunkID = buffer.getLong();
                    if (versions[(int) (chunkID - p_offset) * 2] == -1) {
                        counter++;
                    }
                    versions[(int) (chunkID - p_offset) * 2] = buffer.getShort();
                    versions[(int) (chunkID - p_offset) * 2 + 1] = buffer.get() << 16 | buffer.get() << 8 | buffer.get();
                }

                if (counter * SSD_ENTRY_SIZE < length) {
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
                        if (versions[(int) (chunkID - p_offset - 1) * 2] == -1) {
                            counter++;
                        }

                        // ChunkID: -1 because 1 is added before putting to avoid LID 0; epoch was incremented before
                        versions[(int) (chunkID - p_offset - 1) * 2] = (short) (m_epoch - 1 + (m_eon << 15));
                        versions[(int) (chunkID - p_offset - 1) * 2 + 1] = oldTable[i + 2];
                    }
                }
                update = true;
                m_logCount = counter;
            } else {
                m_logCount = counter;
                m_accessLock.unlock();
            }

            if (p_writeBack && update) {
                // Write back current hashtable compactified
                data = new byte[counter * SSD_ENTRY_SIZE];
                buffer = ByteBuffer.wrap(data);

                for (int i = 0; i < versions.length; i += 2) {
                    chunkID = i / 2 + p_offset;
                    if (versions[i] != -1) {
                        // ChunkID (-1 because 1 is added before putting to avoid LID 0)
                        buffer.putLong(chunkID);
                        // Epoch (4 Bytes in hashtable, 2 in persistent table)
                        buffer.putShort((short) versions[i]);
                        // Version (4 Bytes in hashtable, 3 in persistent table)
                        version = versions[i + 1];
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
            // #if LOGGER >= ERROR
            LOGGER.error("Could write to versions file", e);
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Read all versions from SSD, add current versions and write back (if specified, only)
     * Is used for secondary logs storing migrations, as ChunkIDs are not in a specific range.
     *
     * @param p_allVersions
     *     the VersionsHashTable to put all current versions in
     * @param p_writeBack
     *     whether the versions should be written-back for compactification
     * @return the versions (might have been enlarged)
     */
    private short readAllMigrations(final TemporaryVersionsStorage p_allVersions, final boolean p_writeBack) {
        short ret;
        int length;
        int version;
        boolean update = false;
        long chunkID;
        byte[] data;
        int[] newTable;
        int[] oldTable;
        int[] hashTable;
        ByteBuffer buffer;
        VersionsHashTable versions;

        versions = p_allVersions.getVersionsForMigrationLog();
        if (m_logCount > versions.capacity()) {
            // There may too many versions in versions log to fit in hashtable -> resize
            versions = p_allVersions.resizeVersionsForMigrationLog(m_logCount);
        }

        ret = m_epoch;
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
                    versions.put(buffer.getLong(), buffer.getShort(), buffer.get() << 16 | buffer.get() << 8 | buffer.get());
                }

                if (versions.size() * SSD_ENTRY_SIZE < length) {
                    // Versions log was not empty -> compact
                    update = true;
                }
            } else {
                // There is nothing on SSD yet
            }

            if (versions.size() + m_count > versions.capacity()) {
                // There may too many versions in versions buffer to fit in hashtable -> resize
                hashTable = versions.getTable();
                versions = p_allVersions.resizeVersionsForMigrationLog(versions.size() + m_count);

                // We need to rehash now
                for (int i = 0; i < hashTable.length; i += 4) {
                    chunkID = (long) hashTable[i] << 32 | hashTable[i + 1] & 0xFFFFFFFFL;
                    if (chunkID != 0) {
                        // ChunkID (-1 because 1 is added before putting to avoid LID 0)
                        versions.put(chunkID - 1, (short) hashTable[i + 2], hashTable[i + 3]);
                    }
                }
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
                        versions.put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2]);
                    }
                }
                update = true;
                m_logCount = versions.size();
            } else {
                m_logCount = versions.size();
                m_accessLock.unlock();
            }

            if (p_writeBack & update) {
                // Write back current hashtable compactified
                data = new byte[versions.size() * SSD_ENTRY_SIZE];
                buffer = ByteBuffer.wrap(data);

                hashTable = versions.getTable();
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
            // #if LOGGER >= ERROR
            LOGGER.error("Could write to versions file", e);
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Maps the given key to the given value in VersionsBuffer
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_version
     *     the version
     */
    private void putInternal(final long p_key, final int p_version) {
        int index;
        long iter;
        final long key = p_key + 1;

        // Avoid rehashing by waiting
        while (m_count == WAIT_THRESHOLD) {
            m_logComponent.grantAccessToWriterThread();
            Thread.yield();
        }

        index = (hash(key) & 0x7FFFFFFF) % VERSIONS_BUFFER_CAPACITY;

        m_accessLock.lock();
        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                set(index, key, p_version);
                break;
            }
            iter = getKey(++index);
        }
        if (iter == 0) {
            // Key unknown until now
            set(index, key, p_version);
            m_count++;
        }
        m_accessLock.unlock();
    }

    /**
     * Increments the epoch
     *
     * @return whether an overflow occurred or not
     */
    private boolean incrementEpoch() {
        boolean ret = false;

        m_epoch++;

        // Overflow
        if (m_epoch == 0) {
            m_eon ^= 1;
            ret = true;
        }

        return ret;
    }

    /**
     * Gets the key at given index
     *
     * @param p_index
     *     the index in table (-> 3 indices per element)
     * @return the key
     */
    private long getKey(final int p_index) {
        int index;

        index = p_index % VERSIONS_BUFFER_CAPACITY * 3;
        return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
    }

    /**
     * Gets the version at given index
     *
     * @param p_index
     *     the index
     * @return the version
     */
    private int getVersion(final int p_index) {
        return m_table[p_index % VERSIONS_BUFFER_CAPACITY * 3 + 2];
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *     the index
     * @param p_key
     *     the key
     * @param p_version
     *     the version
     */
    private void set(final int p_index, final long p_key, final int p_version) {
        int index;

        index = p_index % VERSIONS_BUFFER_CAPACITY * 3;
        m_table[index] = (int) (p_key >> 32);
        m_table[index + 1] = (int) p_key;
        m_table[index + 2] = p_version;
    }

}