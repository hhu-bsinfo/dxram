/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.dxutils.jni.JNIFileDirect;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;

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
    private static final int READ_BUFFER_CHUNK_SIZE = 4 * 1024 * 1024;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionsBuffer.class.getSimpleName());

    // Attributes
    private static final DirectByteBufferWrapper FLUSH_BUFFER_WRAPPER =
            new DirectByteBufferWrapper(SSD_ENTRY_SIZE * VERSIONS_BUFFER_CAPACITY, true);
    private static final ByteBuffer FLUSH_BUFFER = FLUSH_BUFFER_WRAPPER.getBuffer();

    private static DirectByteBufferWrapper ms_reorgBufferWrapper =
            new DirectByteBufferWrapper(READ_BUFFER_CHUNK_SIZE, true);

    private LogComponent m_logComponent;
    private HarddriveAccessMode m_mode;

    private final short m_originalOwner;
    private int[] m_table;
    private int m_count;
    private int m_intCapacity;
    private byte m_eon;
    private short m_epoch;

    private int m_windowSize;
    private long m_numberOfCIDs = 0;
    private volatile double m_averageLID = 0;

    private String m_path;
    private RandomAccessFile m_versionsFile;
    private int m_fileID;

    private ReentrantLock m_accessLock;

    // Constructors

    /**
     * Creates an instance of VersionsBuffer
     *
     * @param p_originalOwner
     *         the creator of the original backup range
     * @param p_logComponent
     *         the log component to enable calling access granting methods
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_path
     *         the versions file's path
     * @param p_mode
     *         the harddrive access mode
     */
    VersionsBuffer(final short p_originalOwner, final LogComponent p_logComponent, final long p_secondaryLogSize,
            final String p_path, final HarddriveAccessMode p_mode) {
        super();

        m_logComponent = p_logComponent;
        m_mode = p_mode;

        m_originalOwner = p_originalOwner;

        m_windowSize = AbstractSecLogEntryHeader.getMaximumNumberOfVersions(p_secondaryLogSize / 2, 64, false);

        m_count = 0;
        m_intCapacity = VERSIONS_BUFFER_CAPACITY * 3;

        m_table = new int[m_intCapacity];

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

                LOGGER.error("Could not create versions file", e);

            }
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            try {
                final File file = new File(p_path);
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new FileNotFoundException();
                    }
                }
                m_fileID = JNIFileDirect.open(file.getCanonicalPath(), 0, 0);
                if (m_fileID < 0) {
                    throw new IOException("JNI Error: Could not create Version Buffer.");
                }
            } catch (final IOException e) {

                LOGGER.error("Could not create versions file", e);

            }
        } else {
            try {
                final File file = new File(p_path);
                m_fileID = JNIFileRaw.createLog(file.getName(), 0);
                if (m_fileID < 0) {
                    throw new IOException("JNI Error: Could not create Version Buffer.");
                }
            } catch (final IOException e) {

                LOGGER.error("Could not create versions file", e);

            }
        }

        m_accessLock = new ReentrantLock(false);
    }

    /**
     * Hashes the given key with MurmurHash3
     *
     * @param p_key
     *         the key
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

        k1 = (int) ((p_key & 0xff00000000L) + (p_key & 0xff0000000000L) + (p_key & 0xff000000000000L) +
                (p_key & 0xff000000000000L));
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
     * Returns the value to which the specified key is mapped in VersionsBuffer
     *
     * @param p_key
     *         the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in VersionsBuffer
     */

    protected final Version get(final long p_key) {
        Version ret = null;
        int index;
        long iter;
        // Increment key to avoid ChunkID 0
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
     *         the key (is incremented before insertion to avoid 0)
     * @param p_version
     *         the version
     */
    protected final void put(final long p_key, final int p_version) {
        // Avoid rehashing and excessive memory usage by waiting
        if (m_count == WAIT_THRESHOLD) {
            m_logComponent.flushDataToPrimaryLog();
            while (m_count == WAIT_THRESHOLD) {
                Thread.yield();
            }
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

                LOGGER.error("Could not read versions file's size", e);

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
        } else {
            JNIFileRaw.deleteLog(m_fileID);
        }
    }

    /**
     * Maps the given key to the given value in VersionsBuffer if VersionBuffer is not completely filled
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_version
     *         the version
     */
    final void tryPut(final long p_key, final int p_version) {
        if (m_count == WAIT_THRESHOLD) {
            m_logComponent.flushDataToPrimaryLog();

            LOGGER.warn("Could not transfer log entry to new eon as current epoch is full");

            return;
        }

        putInternal(p_key, p_version);
    }

    /**
     * Returns the next value to which the specified key is mapped in VersionsBuffer
     *
     * @param p_key
     *         the searched key (is incremented before insertion to avoid 0)
     * @return the 1 + value to which the key is mapped in VersionsBuffer
     */
    final Version getNext(final long p_key) {
        Version ret = null;
        int index;
        long iter;
        // Increment key to avoid ChunkID 0
        final long key = p_key + 1;

        // Avoid rehashing by waiting
        if (m_count == WAIT_THRESHOLD) {
            m_logComponent.flushDataToPrimaryLog();
            while (m_count == WAIT_THRESHOLD) {
                Thread.yield();
            }
        }

        index = (hash(key) & 0x7FFFFFFF) % VERSIONS_BUFFER_CAPACITY;

        m_accessLock.lock();
        // Determine range barriers for not migrated/recovered chunks
        if (m_originalOwner == ChunkID.getCreatorID(p_key)) {
            long localID = ChunkID.getLocalID(p_key);
            if (m_numberOfCIDs == 0) {
                m_averageLID = localID;
            } else {
                if (m_numberOfCIDs < OUTLIERS_THRESHOLD || Math.abs(localID - m_averageLID) <= m_windowSize) {
                    // If the euclidean distance is less than or equal to the window size, adjust the window
                    // -> Chunks with re-used ChunkIDs are not considered for average calculation
                    // Initially, use all ChunkIDs to reduce probability of a misplaced window because of re-used
                    // ChunkIDs at the beginning
                    m_averageLID += ((double) localID - m_averageLID) / (m_numberOfCIDs + 1);

                    // TODO: can be 0 for range 0 and sequential update pattern
                }
            }
            m_numberOfCIDs++;
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

        // TODO: release lock in a second call after writing the entry to primary log
        m_accessLock.unlock();

        return ret;
    }

    /**
     * Write all versions to SSD and clear hash table
     *
     * @return whether an overflow during incrementation of epoch occurred or not
     */
    final boolean flush() {
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
                FLUSH_BUFFER.position(0);

                // Iterate over all entries (4 bytes per cell)
                for (int i = 0; i < oldTable.length; i += 3) {
                    chunkID = (long) oldTable[i] << 32 | oldTable[i + 1] & 0xFFFFFFFFL;
                    if (chunkID != 0) {
                        // ChunkID (-1 because 1 is added before putting to avoid CID 0)
                        FLUSH_BUFFER.putLong(chunkID - 1);
                        // Epoch (2 Bytes in persistent table; was incremented before!)
                        FLUSH_BUFFER.putShort((short) (m_epoch - 1 + (m_eon << 15)));
                        // Version (4 Bytes in hashtable, 3 in persistent table)
                        version = oldTable[i + 2];
                        FLUSH_BUFFER.put((byte) (version >>> 16));
                        FLUSH_BUFFER.put((byte) (version >>> 8));
                        FLUSH_BUFFER.put((byte) version);
                    }
                }

                if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                    m_versionsFile.seek(m_versionsFile.length());
                    m_versionsFile.write(FLUSH_BUFFER_WRAPPER.getBuffer().array(), 0, count * SSD_ENTRY_SIZE);
                } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                    if (JNIFileDirect
                            .write(m_fileID, FLUSH_BUFFER_WRAPPER.getAddress(), 0, count * SSD_ENTRY_SIZE, -1, (byte) 0,
                                    (byte) 1) < 0) {
                        throw new IOException("JNI Error.");
                    }
                } else {
                    if (JNIFileRaw
                            .write(m_fileID, FLUSH_BUFFER_WRAPPER.getAddress(), 0, count * SSD_ENTRY_SIZE, -1, (byte) 0,
                                    (byte) 1) < 0) {
                        throw new IOException("JNI Error.");
                    }
                }
            } catch (final IOException e) {

                LOGGER.error("Could write to versions file", e);

            }
        } else

        {
            m_accessLock.unlock();
        }

        return ret;
    }

    /**
     * Read all versions from SSD, add current versions and write back (if specified, only)
     *
     * @param p_allVersions
     *         the array and hashtable to put eons, epochs and versions in
     * @param p_writeBack
     *         whether the versions should be written-back for compactification
     * @return the lowest CID at the time the versions are read-in
     */
    long readAll(final TemporaryVersionsStorage p_allVersions, final boolean p_writeBack) {
        int length;
        int version;
        boolean update = false;
        long chunkID;
        long averageLID;
        long lowestCID;
        long lowestLID;
        long highestLID;
        int[] newTable;
        int[] oldTable;
        VersionsHashTable versionsHashTable;
        VersionsArray versionsArray;

        averageLID = (long) m_averageLID;

        versionsHashTable = p_allVersions.getVersionsHashTable();
        versionsArray = p_allVersions.getVersionsArray();
        int size = versionsArray.capacity();
        lowestLID = Math.max(0, averageLID - size / 2);
        highestLID = lowestLID + size - 1;
        lowestCID = ((long) m_originalOwner << 48) + lowestLID;
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
                ByteBuffer readBuffer = ms_reorgBufferWrapper.getBuffer();
                if (length > readBuffer.capacity()) {
                    ms_reorgBufferWrapper = new DirectByteBufferWrapper(
                            length + READ_BUFFER_CHUNK_SIZE - length % READ_BUFFER_CHUNK_SIZE, true);
                    readBuffer = ms_reorgBufferWrapper.getBuffer();
                }

                if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                    m_versionsFile.seek(0);
                    m_versionsFile.readFully(readBuffer.array(), 0, length);
                } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                    if (JNIFileDirect.read(m_fileID, ms_reorgBufferWrapper.getAddress(), 0, length, 0) < 0) {
                        throw new IOException("JNI error: Could not read file");
                    }
                } else {
                    if (JNIFileRaw.read(m_fileID, ms_reorgBufferWrapper.getAddress(), 0, length, 0) < 0) {
                        throw new IOException("JNI error: Could not read file");
                    }
                }

                if (!Thread.currentThread().isInterrupted()) {
                    readBuffer.clear();

                    for (int i = 0; i * SSD_ENTRY_SIZE < length; i++) {
                        chunkID = readBuffer.getLong();

                        long localID = ChunkID.getLocalID(chunkID);
                        if (localID >= lowestLID && localID <= highestLID) {
                            // ChunkID is in range -> put in array
                            versionsArray.put(chunkID, readBuffer.getShort(),
                                    (readBuffer.get() & 0xFF) << 16 | (readBuffer.get() & 0xFF) << 8 |
                                            readBuffer.get() & 0xFF, lowestCID);
                        } else {
                            // ChunkID is outside of range -> put in hashtable
                            versionsHashTable.put(chunkID, readBuffer.getShort(),
                                    (readBuffer.get() & 0xFF) << 16 | (readBuffer.get() & 0xFF) << 8 |
                                            readBuffer.get() & 0xFF);
                        }
                    }

                    if ((versionsArray.size() + versionsHashTable.size()) * SSD_ENTRY_SIZE < length) {
                        // Versions log was not empty -> compact
                        update = true;
                    }
                }
            } else {
                // There is nothing on SSD yet
            }

            if (!Thread.currentThread().isInterrupted()) {
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
                            // ChunkID (-1 because 1 is added before putting to avoid CID 0);
                            // epoch was incremented before
                            long localID = ChunkID.getLocalID(chunkID - 1);
                            if (localID >= lowestLID && localID <= highestLID) {
                                // ChunkID is in range -> put in array
                                versionsArray.put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2],
                                        lowestCID);
                            } else {
                                // ChunkID is outside of range -> put in hashtable
                                versionsHashTable
                                        .put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2]);
                            }
                        }
                    }
                    update = true;
                } else {
                    m_accessLock.unlock();
                }
            }

            if (!Thread.currentThread().isInterrupted()) {
                if (p_writeBack && update) {
                    // Write back current hashtable compactified
                    length = (versionsArray.size() + versionsHashTable.size()) * SSD_ENTRY_SIZE;
                    ByteBuffer writeBuffer = ms_reorgBufferWrapper.getBuffer();
                    if (length > writeBuffer.capacity()) {
                        ms_reorgBufferWrapper = new DirectByteBufferWrapper(
                                length + READ_BUFFER_CHUNK_SIZE - length % READ_BUFFER_CHUNK_SIZE, true);
                        writeBuffer = ms_reorgBufferWrapper.getBuffer();
                    }
                    writeBuffer.clear();

                    // Gather all entries from array
                    for (int i = 0; i < versionsArray.capacity(); i++) {
                        chunkID = i + lowestCID;
                        version = versionsArray.getVersion(i, 0);

                        if (version != -1) {
                            writeBuffer.putLong(chunkID);
                            // Epoch (4 Bytes in hashtable, 2 in persistent table)
                            writeBuffer.putShort(versionsArray.getEpoch(i, 0));
                            // Version (4 Bytes in hashtable, 3 in persistent table)
                            writeBuffer.put((byte) (version >>> 16));
                            writeBuffer.put((byte) (version >>> 8));
                            writeBuffer.put((byte) version);
                        }
                    }

                    // Gather all entries from hashtable
                    int[] hashTable = versionsHashTable.getTable();
                    for (int i = 0; i < hashTable.length; i += 4) {
                        chunkID = (long) hashTable[i] << 32 | hashTable[i + 1] & 0xFFFFFFFFL;
                        if (chunkID != 0) {
                            // ChunkID (-1 because 1 is added before putting to avoid CID 0)
                            writeBuffer.putLong(chunkID - 1);
                            // Epoch (4 Bytes in hashtable, 2 in persistent table)
                            writeBuffer.putShort((short) hashTable[i + 2]);
                            // Version (4 Bytes in hashtable, 3 in persistent table)
                            version = hashTable[i + 3];
                            writeBuffer.put((byte) (version >>> 16));
                            writeBuffer.put((byte) (version >>> 8));
                            writeBuffer.put((byte) version);
                        }
                    }

                    if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                        m_versionsFile.seek(0);
                        m_versionsFile.write(writeBuffer.array());
                        m_versionsFile.setLength(length);
                    } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                        if (JNIFileDirect
                                .write(m_fileID, ms_reorgBufferWrapper.getAddress(), 0, length, 0, (byte) 0, (byte) 1) <
                                0) {
                            throw new IOException("JNI error: Could not write to file");
                        }
                    } else {
                        if (JNIFileRaw
                                .write(m_fileID, ms_reorgBufferWrapper.getAddress(), 0, length, 0, (byte) 0, (byte) 1) <
                                0) {
                            throw new IOException("JNI error: Could not write to file");
                        }
                    }
                }
            }
        } catch (final IOException e) {

            LOGGER.error("Could not update versions file", e);

        }

        return lowestCID;
    }

    /**
     * Maps the given key to the given value in VersionsBuffer
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_version
     *         the version
     */
    private void putInternal(final long p_key, final int p_version) {
        int index;
        long iter;
        // Increment key to avoid ChunkID 0
        final long key = p_key + 1;

        // Avoid rehashing by waiting
        while (m_count == WAIT_THRESHOLD) {
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
     *         the index in table (-> 3 indices per element)
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
     *         the index
     * @return the version
     */
    private int getVersion(final int p_index) {
        return m_table[p_index % VERSIONS_BUFFER_CAPACITY * 3 + 2];
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *         the index
     * @param p_key
     *         the key
     * @param p_version
     *         the version
     */
    private void set(final int p_index, final long p_key, final int p_version) {
        int index;

        index = p_index % VERSIONS_BUFFER_CAPACITY * 3;
        m_table[index] = (int) (p_key >> 32);
        m_table[index + 1] = (int) p_key;
        m_table[index + 2] = p_version;
    }
}
