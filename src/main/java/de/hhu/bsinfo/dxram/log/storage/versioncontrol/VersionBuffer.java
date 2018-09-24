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

package de.hhu.bsinfo.dxram.log.storage.versioncontrol;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxutils.hashtable.LongHashTable;
import de.hhu.bsinfo.dxutils.hashtable.LongIntHashTable;

/**
 * The version buffer, based on a hash table to store versions (Linear probing).
 * The version buffer is flushed based on the threshold and cleared during reorganization.
 * The version log is created and accessed by this class.
 * Note: the version buffer extends LongIntHashTable. Thus storing versions for one chunk requires 12 bytes
 * in the hash table (8 bytes for the chunk ID, 4 bytes for the version, epoch is stored once per instance of
 * VersionBuffer). The total size needed in log is 13 bytes (8 bytes for chunk ID, 3 bytes for version, 2 bytes for
 * epoch). The version hash table used for reorganization and recovery uses 16 bytes to store chunk ID, version
 * tuples (8 bytes for chunk ID, 8 bytes for version + epoch).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2016
 */
public final class VersionBuffer extends LongIntHashTable {

    private static final int VERSIONS_BUFFER_CAPACITY = 262144;
    private static final float FLUSH_FACTOR = 0.65f;
    private static final int FLUSH_THRESHOLD = (int) (VERSIONS_BUFFER_CAPACITY * FLUSH_FACTOR);
    private static final int SSD_ENTRY_SIZE = 13;
    private static final int OUTLIERS_THRESHOLD = 1000;
    private static final int READ_BUFFER_CHUNK_SIZE = 4 * 1024 * 1024;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionBuffer.class.getSimpleName());

    private static final DirectByteBufferWrapper FLUSH_BUFFER_WRAPPER =
            new DirectByteBufferWrapper(SSD_ENTRY_SIZE * VERSIONS_BUFFER_CAPACITY, true);
    private static final ByteBuffer FLUSH_BUFFER = FLUSH_BUFFER_WRAPPER.getBuffer();

    private static DirectByteBufferWrapper ms_reorgBufferWrapper =
            new DirectByteBufferWrapper(READ_BUFFER_CHUNK_SIZE, true);

    private final Scheduler m_scheduler;

    private VersionLog m_log;

    private final short m_originalOwner;
    private final int m_windowSize;

    private byte m_eon;
    private short m_epoch;

    private long m_numberOfCIDs = 0;

    private volatile double m_averageLID = 0;

    private ReentrantLock m_accessLock;

    /**
     * Creates an instance of VersionBuffer.
     *
     * @param p_scheduler
     *         the scheduler for triggering a write buffer flush if version buffer is full
     * @param p_originalOwner
     *         the creator of the original backup range
     * @param p_backupRangeSize
     *         the backup range size
     * @param p_logFileName
     *         the file name (including backup directory) of the version log
     */
    VersionBuffer(final Scheduler p_scheduler, final short p_originalOwner, final long p_backupRangeSize,
            final String p_logFileName) {
        super(VERSIONS_BUFFER_CAPACITY);

        m_scheduler = p_scheduler;

        m_originalOwner = p_originalOwner;

        m_windowSize = AbstractSecLogEntryHeader.getMaximumNumberOfVersions(p_backupRangeSize, 64, false);

        m_eon = 0;
        m_epoch = 0;

        try {
            m_log = new VersionLog(new File(p_logFileName));
        } catch (final IOException e) {
            LOGGER.error("Could not create version log.", e);
        }

        m_accessLock = new ReentrantLock(false);
    }

    /**
     * Closes the version buffer and version log.
     */
    public void close() throws IOException {
        m_log.close();
    }

    /**
     * Closes the version buffer and deletes the version log.
     */
    public void closeAndRemoveLog() throws IOException {
        m_log.closeAndRemove();
    }

    /**
     * Returns the number of keys in VersionBuffer.
     *
     * @return the number of keys in VersionBuffer
     */

    public final boolean isThresholdReached() {
        return size() >= FLUSH_THRESHOLD;
    }

    /**
     * Returns the number of keys in VersionBuffer.
     *
     * @return the number of keys in VersionBuffer
     */
    public final long getLogFileSize() {
        return m_log.getFileSize();
    }

    /**
     * Returns the current epoch.
     *
     * @return the current epoch
     */
    public final short getEpoch() {
        return m_epoch;
    }

    /**
     * Returns the current eon.
     *
     * @return the current eon
     */
    public final byte getEon() {
        return m_eon;
    }

    /**
     * Update metadata after recovery.
     *
     * @param p_newFile
     *         the new file name
     */
    public void transferBackupRange(final String p_newFile) throws IOException {
        m_log.transferBackupRange(p_newFile);
    }

    /**
     * Returns the value to which the specified key is mapped in VersionBuffer.
     *
     * @param p_key
     *         the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in VersionBuffer
     */
    public final Version getVersion(final long p_key) {

        m_accessLock.lock();
        // Increment key to avoid ChunkID 0
        long value = get(p_key + 1);
        m_accessLock.unlock();

        if (value != -1) {
            return new Version((short) (m_epoch + (m_eon << 15)), (int) value);
        }
        return null;
    }

    /**
     * Returns the next value to which the specified key is mapped in VersionBuffer.
     *
     * @param p_key
     *         the searched key (is incremented before insertion to avoid 0)
     * @return the 1 + value to which the key is mapped in VersionBuffer
     */
    final Version getNextVersion(final long p_key) {
        // Avoid rehashing by waiting
        while (isFull()) {
            m_scheduler.flushWriteBuffer();
        }

        m_accessLock.lock(); // do not update version while it is flushed to disk

        updateRangeBarriers(p_key);

        // Increment key to avoid ChunkID 0
        long oldValue = add(p_key + 1, 1);

        // TODO: release lock in a second call after writing the entry to primary log
        m_accessLock.unlock();

        if (oldValue != -1) {
            return new Version((short) (m_epoch + (m_eon << 15)), (int) (oldValue + 1));
        }
        return new Version((short) (m_epoch + (m_eon << 15)), 1);
    }

    /**
     * Updates the range barriers.
     * Assumes
     *
     * @param p_key
     *         the chunk ID to be inserted
     */
    private void updateRangeBarriers(final long p_key) {
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
                    m_averageLID += ((double) localID - m_averageLID) /
                            (m_numberOfCIDs + 1); // Changed by exclusive message handler, only

                    // TODO: can be 0 for range 0 and sequential update pattern
                }
            }
            m_numberOfCIDs++;
        }
    }

    /**
     * Maps the given key to the given value in VersionBuffer.
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_version
     *         the version
     */
    final void putVersion(final long p_key, final int p_version) {
        // Avoid rehashing and excessive memory usage by waiting
        while (isFull()) {
            m_scheduler.flushWriteBuffer();
        }

        m_accessLock.lock();
        // Increment key to avoid ChunkID 0
        put(p_key + 1, p_version);
        m_accessLock.unlock();
    }

    /**
     * Maps the given key to the given value in VersionBuffer if VersionBuffer is not completely filled.
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_version
     *         the version
     */
    public final void tryPut(final long p_key, final int p_version) {
        if (isFull()) {
            m_scheduler.flushWriteBuffer();

            LOGGER.warn("Could not transfer log entry to new eon as current epoch is full");

            return;
        }

        m_accessLock.lock();
        // Increment key to avoid ChunkID 0
        put(p_key + 1, p_version);
        m_accessLock.unlock();
    }

    /**
     * Writes all versions to SSD and clears the hash table.
     *
     * @return whether an overflow during incrementation of epoch occurred or not
     */
    public final boolean flush() {
        boolean ret = false;
        long chunkID;
        long version;
        int[] oldTable;

        // Append all new versions to versions file
        m_accessLock.lock();
        int count = size();
        if (count > 0) {
            oldTable = replace();

            ret = incrementEpoch();
            m_accessLock.unlock();

            try {
                // Re-use ByteBuffer
                FLUSH_BUFFER.position(0);

                // Iterate over all entries
                for (int i = 0; i < oldTable.length; i += 3) {
                    chunkID = (long) oldTable[i] << 32 | oldTable[i + 1];
                    if (chunkID != 0) {
                        // ChunkID (-1 because 1 is added before putting to avoid CID 0)
                        FLUSH_BUFFER.putLong(chunkID - 1);
                        // Epoch (2 Bytes in persistent table; was incremented before!)
                        FLUSH_BUFFER.putShort((short) (m_epoch - 1 + (m_eon << 15)));
                        // Version (8 Bytes in hash table, 3 in persistent table)
                        version = oldTable[i + 2];
                        FLUSH_BUFFER.put((byte) (version >>> 16));
                        FLUSH_BUFFER.put((byte) (version >>> 8));
                        FLUSH_BUFFER.put((byte) version);
                    }
                }

                m_log.appendToLog(FLUSH_BUFFER_WRAPPER, 0, count * SSD_ENTRY_SIZE);

            } catch (final IOException e) {
                LOGGER.error("Could write to versions file", e);
            }
        } else {
            m_accessLock.unlock();
        }

        return ret;
    }

    /**
     * Reads all versions from SSD, adds current versions and writes back (if specified, only).
     * Note: if the reorganization thread executes this method, it might get interrupted to clear the way for the
     * recovery.
     *
     * @param p_allVersions
     *         the array and hash table to put eons, epochs and versions in
     * @param p_writeBack
     *         whether the versions should be written-back for compactification
     * @return the lowest CID at the time the versions are read-in
     * @throws IOException
     *         if versions could not be read from log
     */
    long readAll(final TemporaryVersionStorage p_allVersions, final boolean p_writeBack) throws IOException {
        boolean update;
        LongHashTable versionHashTable = p_allVersions.getVersionsHashTable();
        VersionArray versionArray = p_allVersions.getVersionsArray();

        // Access locking:
        // We lock the access to the version buffer (in migrateVersionsFromVersionBuffer()), only. Reading from log
        // and writing back, as well as, the range barrier determination is not locked.
        // This method cannot be called concurrently because either the reorganization thread or the recovery thread
        // (message handler) call this method. Before recovering, the recovery thread blocks the reorganization
        // thread and enters this area after the reorganization thread left.
        // The version buffer is not flushed in parallel as a secondary log marked for reorganization cannot be
        // flushed. Instead the reorganization is triggered.
        // The range barriers might be a little off during the recovery because they are determined at the beginning
        // of this method and the version buffer might be filled concurrently. This could affect the performance a
        // little, as the hash table is used instead of the array, but not nearly as much as locking the entire area.

        // Determine range barriers
        long averageLID = (long) m_averageLID;
        int size = versionArray.capacity();
        long lowestLID = Math.max(0, averageLID - size / 2);
        long highestLID = lowestLID + size - 1;
        long lowestCID = ((long) m_originalOwner << 48) + lowestLID;

        try {
            update = readFromLog(lowestLID, highestLID, lowestCID, versionArray, versionHashTable);
        } catch (final IOException e) {
            LOGGER.error("Could not read from version log", e);
            throw e;
        }

        if (!Thread.currentThread().isInterrupted()) {
            update = migrateVersionsFromVersionBuffer(lowestLID, highestLID, lowestCID, versionArray, versionHashTable,
                    update);
        }

        if (!Thread.currentThread().isInterrupted()) {
            if (p_writeBack && update) {
                try {
                    writeBackToLog(lowestCID, versionArray, versionHashTable);
                } catch (final IOException e) {
                    LOGGER.error("Could not write to version log", e);
                }
            }
        }

        return lowestCID;
    }

    /**
     * Reads all versions from version log and puts them into the version array (versions within range barriers) or
     * version hash table (outside range like migrations).
     *
     * @param p_lowestLID
     *         the lower range barrier
     * @param p_highestLID
     *         the upper range barrier
     * @param p_lowestCID
     *         the lower range barrier + creator
     * @param p_versionArray
     *         the version array
     * @param p_versionHashTable
     *         the version hash table
     * @return true if version data structures have been updated
     * @throws IOException
     *         if the version log could not be read
     */
    private boolean readFromLog(final long p_lowestLID, final long p_highestLID, final long p_lowestCID,
            final VersionArray p_versionArray, final LongHashTable p_versionHashTable) throws IOException {
        boolean ret = false;
        int length = (int) getLogFileSize();

        if (length > 0) {
            // Read all entries from SSD to hash table

            // Read old versions from SSD and add to hash table
            // Then read all new versions from versions log and add to hash table (overwrites older entries!)
            ByteBuffer readBuffer = ms_reorgBufferWrapper.getBuffer();
            if (length > readBuffer.capacity()) {
                ms_reorgBufferWrapper =
                        new DirectByteBufferWrapper(length + READ_BUFFER_CHUNK_SIZE - length % READ_BUFFER_CHUNK_SIZE,
                                true);
                readBuffer = ms_reorgBufferWrapper.getBuffer();
            }

            m_log.readFromLog(ms_reorgBufferWrapper, length, 0);

            if (!Thread.currentThread().isInterrupted()) {
                readBuffer.clear();

                for (int i = 0; i * SSD_ENTRY_SIZE < length; i++) {
                    long chunkID = readBuffer.getLong();

                    long localID = ChunkID.getLocalID(chunkID);
                    if (localID >= p_lowestLID && localID <= p_highestLID) {
                        // ChunkID is in range -> put in array
                        p_versionArray.put(chunkID, readBuffer.getShort(),
                                (readBuffer.get() & 0xFF) << 16 | (readBuffer.get() & 0xFF) << 8 |
                                        readBuffer.get() & 0xFF, p_lowestCID);
                    } else {
                        // ChunkID is outside of range -> put in hash table (converts three bytes of version and
                        // two bytes of epoch to one long)
                        p_versionHashTable.put(chunkID + 1,
                                (long) (readBuffer.getShort() & 0xFFFF) << 32 | (readBuffer.get() & 0xFF) << 16 |
                                        (readBuffer.get() & 0xFF) << 8 | readBuffer.get() & 0xFF);
                    }
                }

                if ((p_versionArray.size() + p_versionHashTable.size()) * SSD_ENTRY_SIZE < length) {
                    // Versions log was not empty -> compact
                    ret = true;
                }
            }
        } else {
            // There is nothing on SSD yet
        }

        return ret;
    }

    /**
     * Migrates all versions in version buffer to version array (versions within range barriers) and hash table
     * (outside range like migrations).
     *
     * @param p_lowestLID
     *         the lower range barrier
     * @param p_highestLID
     *         the upper range barrier
     * @param p_lowestCID
     *         the lower range barrier + creator
     * @param p_versionArray
     *         the version array
     * @param p_versionHashTable
     *         the version hash table
     * @return true if version data structures have been updated
     */
    private boolean migrateVersionsFromVersionBuffer(final long p_lowestLID, final long p_highestLID,
            final long p_lowestCID, final VersionArray p_versionArray, final LongHashTable p_versionHashTable,
            final boolean p_update) {
        boolean ret = p_update;
        int[] oldTable;

        // Put all versions from versions buffer to VersionHashTable
        m_accessLock.lock();
        if (size() > 0) {
            oldTable = replace();

            incrementEpoch();
            m_accessLock.unlock();

            for (int i = 0; i < oldTable.length; i += 3) {
                long chunkID = (long) oldTable[i] << 32 | oldTable[i + 1];
                if (chunkID != 0) {
                    // ChunkID (-1 because 1 is added before putting to avoid CID 0);
                    // epoch was incremented before
                    long localID = ChunkID.getLocalID(chunkID - 1);
                    if (localID >= p_lowestLID && localID <= p_highestLID) {
                        // ChunkID is in range -> put in array
                        p_versionArray
                                .put(chunkID - 1, (short) (m_epoch - 1 + (m_eon << 15)), oldTable[i + 2], p_lowestCID);
                    } else {
                        // ChunkID is outside of range -> put in hashtable
                        p_versionHashTable.put(chunkID, (long) (m_epoch - 1 + (m_eon << 15)) << 32 | oldTable[i + 2]);
                    }
                }
            }
            ret = true;
        } else {
            m_accessLock.unlock();
        }

        return ret;
    }

    /**
     * Increments the epoch.
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
     * Writes the version data structures (array and hash table) to version log. Outdated entries have been disposed
     * during the filling of the data structures. Therefore, the versions are written to log in the most compact way.
     *
     * @param p_lowestCID
     *         the lower range barrier + creator
     * @param p_versionArray
     *         the version array
     * @param p_versionHashTable
     *         the version hash table
     * @throws IOException
     *         if versions could not be written to version log
     */
    private void writeBackToLog(final long p_lowestCID, final VersionArray p_versionArray,
            final LongHashTable p_versionHashTable) throws IOException {

        // Write back current hash table compactified
        int length = (p_versionArray.size() + p_versionHashTable.size()) * SSD_ENTRY_SIZE;
        ByteBuffer writeBuffer = ms_reorgBufferWrapper.getBuffer();
        if (length > writeBuffer.capacity()) {
            ms_reorgBufferWrapper =
                    new DirectByteBufferWrapper(length + READ_BUFFER_CHUNK_SIZE - length % READ_BUFFER_CHUNK_SIZE,
                            true);
            writeBuffer = ms_reorgBufferWrapper.getBuffer();
        }
        writeBuffer.clear();

        // Gather all entries from array
        for (int i = 0; i < p_versionArray.capacity(); i++) {
            long chunkID = i + p_lowestCID;
            int version = p_versionArray.getVersion(i, 0);

            if (version != -1) {
                writeBuffer.putLong(chunkID);
                // Epoch (4 Bytes in hash table, 2 in persistent table)
                writeBuffer.putShort(p_versionArray.getEpoch(i, 0));
                // Version (4 Bytes in hash table, 3 in persistent table)
                writeBuffer.put((byte) (version >>> 16));
                writeBuffer.put((byte) (version >>> 8));
                writeBuffer.put((byte) version);
            }
        }

        // Gather all entries from hash table
        long[] hashTable = p_versionHashTable.getTable();
        for (int i = 0; i < hashTable.length; i += 2) {
            long chunkID = hashTable[i];
            if (chunkID != 0) {
                // ChunkID (-1 because 1 is added before putting to avoid CID 0)
                writeBuffer.putLong(chunkID - 1);
                // Epoch (4 Bytes in hash table, 2 in persistent table)
                writeBuffer.putShort((short) (hashTable[i + 1] >> 32));
                // Version (4 Bytes in hash table, 3 in persistent table)
                int version = (int) hashTable[i + 1];
                writeBuffer.put((byte) (version >>> 16));
                writeBuffer.put((byte) (version >>> 8));
                writeBuffer.put((byte) version);
            }
        }

        m_log.writeToLog(ms_reorgBufferWrapper, 0, 0, length, false);
    }

}
