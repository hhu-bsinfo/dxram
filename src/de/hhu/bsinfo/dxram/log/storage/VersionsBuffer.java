/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.utils.JNIFileDirect;
import de.hhu.bsinfo.utils.JNIFileRaw;

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
    private static final int OUTLIERS_THRESHOLD = 1000;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionsBuffer.class.getSimpleName());

    // Attributes
    private static ByteBuffer ms_flushBuffer;

    private LogComponent m_logComponent;
    private HarddriveAccessMode m_mode;

    private int[] m_table;
    private int m_count;
    private int m_intCapacity;
    private byte m_eon;
    private short m_epoch;

    private int m_windowSize;
    private long m_numberOfLIDs = 0;
    private double m_averageLID = 0;

    private String m_path;
    private RandomAccessFile m_versionsFile;
    private int m_fileID;
    // Pointer to read- and writebuffer for JNI
    private long m_readBufferPointer;
    private long m_writeBufferPointer;
    // Size for read and write JNI-buffer
    private int m_readBufferSize;
    private int m_writeBufferSize;

    private ReentrantLock m_accessLock;

    // Constructors

    /**
     * Creates an instance of VersionsBuffer
     *
     * @param p_logComponent
     *     the log component to enable calling access granting methods
     * @param p_secondaryLogSize
     *     the secondary log size
     * @param p_path
     *     the versions file's path
     * @param p_mode
     *     the harddrive access mode
     */
    VersionsBuffer(final LogComponent p_logComponent, final long p_secondaryLogSize, final String p_path, final HarddriveAccessMode p_mode) {
        super();

        m_logComponent = p_logComponent;
        m_mode = p_mode;

        m_windowSize = AbstractSecLogEntryHeader.getMaximumNumberOfVersions(p_secondaryLogSize / 2, 64, false);

        m_count = 0;
        m_intCapacity = VERSIONS_BUFFER_CAPACITY * 3;

        m_table = new int[m_intCapacity];
        ms_flushBuffer = ByteBuffer.allocate(SSD_ENTRY_SIZE * VERSIONS_BUFFER_CAPACITY);

        m_eon = 0;
        m_epoch = 0;

        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            m_path = p_path;
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
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            try {
                final File file = new File(p_path);
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new FileNotFoundException();
                    }
                }
                m_fileID = JNIFileDirect.open(file.getCanonicalPath(), 0);
                if (m_fileID < 0) {
                    throw new IOException("JNI Error: Could not create Version Buffer.");
                }
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not create versions file", e);
                // #endif /* LOGGER >= ERROR */
            }
            // Set size for JNI-buffer (use 4 MB as standard)
            // Use only multiples of the device-blocksize to get an aligned buffer!
            m_writeBufferSize = 4 * 1024 * 1024;
            m_readBufferSize = 4 * 1024 * 1024;
            // Set pointer to buffer to 0
            m_readBufferPointer = JNIFileDirect.createBuffer(m_readBufferSize);
            m_writeBufferPointer = JNIFileDirect.createBuffer(m_writeBufferSize);
        } else {
            try {
                final File file = new File(p_path);
                m_fileID = JNIFileRaw.createLog(file.getName(), 0);
                if (m_fileID < 0) {
                    throw new IOException("JNI Error: Could not create Version Buffer.");
                }
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not create versions file", e);
                // #endif /* LOGGER >= ERROR */
            }
            // Set size for JNI-buffer (use 4 MB as standard)
            // Use only multiples of the device-blocksize to get an aligned buffer!
            m_writeBufferSize = 4 * 1024 * 1024;
            m_readBufferSize = 4 * 1024 * 1024;
            // Set pointer to buffer to 0
            m_readBufferPointer = JNIFileDirect.createBuffer(m_readBufferSize);
            m_writeBufferPointer = JNIFileDirect.createBuffer(m_writeBufferSize);
        }

        m_accessLock = new ReentrantLock(false);
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

    // Getter

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

                if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                    m_versionsFile.seek((int) m_versionsFile.length());
                    m_versionsFile.write(ms_flushBuffer.array(), 0, count * SSD_ENTRY_SIZE);
                } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                    if (JNIFileDirect
                        .dwrite(m_fileID, ms_flushBuffer.array(), 0, count * SSD_ENTRY_SIZE, (int) JNIFileDirect.length(m_fileID), m_writeBufferPointer,
                            m_writeBufferSize) < 0) {
                        throw new IOException("JNI Error.");
                    }
                } else {
                    long writePosition = JNIFileRaw.dlength(m_fileID);
                    if (JNIFileRaw.dwrite(m_fileID, ms_flushBuffer.array(), 0, count * SSD_ENTRY_SIZE, writePosition, m_writeBufferPointer, m_writeBufferSize) <
                        0) {
                        throw new IOException("JNI Error.");
                    }
                }
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

        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            try {
                return m_versionsFile.length();
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not read versions file's size", e);
                // #endif /* LOGGER >= ERROR */
                return -1;
            }
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            return JNIFileDirect.length(m_fileID);
        } else {
            return JNIFileRaw.dlength(m_fileID);
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

    // Methods

    /**
     * Closes the version buffer and deletes the version log
     */
    void closeAndRemove() throws IOException {
        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            final File file = new File(m_path);
            if (file.exists()) {
                if (!file.delete()) {
                    throw new FileNotFoundException();
                }
            }
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            final File file = new File(m_path);
            if (file.exists()) {
                if (!file.delete()) {
                    throw new FileNotFoundException();
                }
            }
            JNIFileDirect.freeBuffer(m_readBufferSize);
            JNIFileDirect.freeBuffer(m_writeBufferSize);
        } else {
            JNIFileRaw.deleteLog(m_fileID);
            JNIFileDirect.freeBuffer(m_readBufferSize);
            JNIFileDirect.freeBuffer(m_writeBufferSize);
        }
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
     * Returns the next value to which the specified key is mapped in VersionsBuffer
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @param p_owner
     *     the owner
     * @return the 1 + value to which the key is mapped in VersionsBuffer
     */
    final Version getNext(final long p_key, final short p_owner) {
        Version ret = null;
        int index;
        long localID = -1;
        long iter;
        long key;

        // Avoid rehashing by waiting
        while (m_count == WAIT_THRESHOLD) {
            m_logComponent.grantAccessToWriterThread();
            Thread.yield();
        }

        if (p_owner == ChunkID.getCreatorID(p_key)) {
            // Use localID for not migrated/recovered chunks
            localID = ChunkID.getLocalID(p_key);
            key = localID + 1;
        } else {
            // Use complete ChunkID for migrated/recovered chunks
            key = p_key + 1;
        }

        index = (hash(key) & 0x7FFFFFFF) % VERSIONS_BUFFER_CAPACITY;

        m_accessLock.lock();
        // Determine range barriers for not migrated/recovered chunks
        if (localID != -1) {
            if (m_numberOfLIDs == 0) {
                m_averageLID = localID;
            } else {
                if (m_numberOfLIDs < OUTLIERS_THRESHOLD || Math.abs(localID - m_averageLID) <= m_windowSize) {
                    // If the euclidean distance is less than or equal to the window size, adjust the window
                    // -> Chunks with re-used ChunkIDs are not considered for average calculation
                    // Initially, use all ChunkIDs to reduce probability of a misplaced window because of re-used ChunkIDs at the beginning
                    m_averageLID += ((double) localID - m_averageLID) / (m_numberOfLIDs + 1);
                }
            }
            m_numberOfLIDs++;
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
     *
     * @param p_allVersions
     *     the array and hashtable to put eons, epochs and versions in
     * @param p_writeBack
     *     whether the versions should be written-back for compactification
     * @return the lowest LID at the time the versions are read-in
     */
    long readAll(final TemporaryVersionsStorage p_allVersions, final boolean p_writeBack) {
        int length;
        int version;
        boolean update = false;
        long chunkID;
        long averageLID;
        long lowestLID;
        long highestLID;
        byte[] data;
        int[] newTable;
        int[] oldTable;
        ByteBuffer buffer;
        VersionsHashTable versionsHashTable;
        VersionsArray versionsArray;

        m_accessLock.lock();
        averageLID = (long) m_averageLID;
        m_accessLock.unlock();

        versionsHashTable = p_allVersions.getVersionsHashTable();
        versionsArray = p_allVersions.getVersionsArray();
        int size = versionsArray.capacity();
        lowestLID = Math.max(0, averageLID - size / 2);
        highestLID = lowestLID + size - 1;

        try {
            if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                length = (int) m_versionsFile.length();
            } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                length = (int) JNIFileDirect.length(m_fileID);
            } else {
                length = (int) JNIFileRaw.dlength(m_fileID);
            }

            if (length > 0) {
                // Read all entries from SSD to hashtable

                // Read old versions from SSD and add to hashtable
                // Then read all new versions from versions log and add to hashtable (overwrites older entries!)
                data = new byte[length];

                if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                    m_versionsFile.seek(0);
                    m_versionsFile.readFully(data);
                } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                    if (JNIFileDirect.read(m_fileID, data, 0, data.length, 0, m_readBufferPointer, m_readBufferSize) < 0) {
                        throw new IOException("JNI error: Could not read file");
                    }
                } else {
                    if (JNIFileRaw.dread(m_fileID, data, 0, data.length, 0, m_readBufferPointer, m_readBufferSize) < 0) {
                        throw new IOException("JNI error: Could not read file");
                    }
                }

                buffer = ByteBuffer.wrap(data);

                for (int i = 0; i * SSD_ENTRY_SIZE < length; i++) {
                    chunkID = buffer.getLong();
                    if (chunkID >= lowestLID && chunkID <= highestLID) {
                        // ChunkID is in range -> put in array
                        versionsArray.put(chunkID, buffer.getShort(), buffer.get() << 16 | buffer.get() << 8 | buffer.get(), lowestLID);
                    } else {
                        // ChunkID is outside of range -> put in hashtable
                        versionsHashTable.put(chunkID, buffer.getShort(), buffer.get() << 16 | buffer.get() << 8 | buffer.get());
                    }
                }

                if ((versionsArray.size() + versionsHashTable.size()) * SSD_ENTRY_SIZE < length) {
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
                        if (chunkID - 1 >= lowestLID && chunkID - 1 <= highestLID) {
                            // ChunkID is in range -> put in array
                            versionsArray.put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2], lowestLID);
                        } else {
                            // ChunkID is outside of range -> put in hashtable
                            versionsHashTable.put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2]);
                        }
                    }
                }
                update = true;
            } else {
                m_accessLock.unlock();
            }

            if (p_writeBack && update) {
                // Write back current hashtable compactified
                data = new byte[(versionsArray.size() + versionsHashTable.size()) * SSD_ENTRY_SIZE];
                buffer = ByteBuffer.wrap(data);

                // Gather all entries from array
                short epoch;
                for (int i = 0; i < versionsArray.capacity(); i++) {
                    chunkID = i + lowestLID;
                    epoch = versionsArray.getEpoch(i, 0);

                    if (epoch != -1) {
                        buffer.putLong(chunkID);
                        // Epoch (4 Bytes in hashtable, 2 in persistent table)
                        buffer.putShort(epoch);
                        // Version (4 Bytes in hashtable, 3 in persistent table)
                        version = versionsArray.getVersion(i, 0);
                        buffer.put((byte) (version >>> 16));
                        buffer.put((byte) (version >>> 8));
                        buffer.put((byte) version);
                    }
                }

                // Gather all entries from hashtable
                int[] hashTable = versionsHashTable.getTable();
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

                if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                    m_versionsFile.seek(0);
                    m_versionsFile.write(data);
                    m_versionsFile.setLength(data.length);
                } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                    if (JNIFileDirect.dwrite(m_fileID, data, 0, data.length, 0, m_writeBufferPointer, m_writeBufferSize) < 0) {
                        throw new IOException("JNI error: Could not write to file");
                    }
                } else {
                    if (JNIFileRaw.dwrite(m_fileID, data, 0, data.length, 0, m_writeBufferPointer, m_writeBufferSize) < 0) {
                        throw new IOException("JNI error: Could not write to file");
                    }
                    if (JNIFileRaw.setDFileLength(m_fileID, data.length) < 0) {
                        throw new IOException("JNI error: Could not set file length");
                    }
                }
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could write to versions file", e);
            // #endif /* LOGGER >= ERROR */
        }

        return lowestLID;
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