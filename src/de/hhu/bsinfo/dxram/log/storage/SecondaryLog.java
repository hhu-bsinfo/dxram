/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent.RecoveryWriterThread;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.JNIFileDirect;
import de.hhu.bsinfo.utils.JNIFileRaw;
import de.hhu.bsinfo.utils.RandomUtils;

/**
 * This class implements the secondary log
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.10.2014
 */
public class SecondaryLog extends AbstractLog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLog.class.getSimpleName());

    // Constants
    private static final String SECLOG_PREFIX_FILENAME = "sec";
    private static final String SECLOG_POSTFIX_FILENAME = ".log";
    private static final byte[] SECLOG_HEADER = "DXRAMSecLogv1".getBytes(Charset.forName("UTF-8"));
    private static final int SORT_THRESHOLD = 100000;

    // Attributes
    private final short m_owner;
    private final short m_rangeID;

    private final long m_secondaryLogReorgThreshold;
    private final long m_secondaryLogSize;
    private final int m_logSegmentSize;
    private final boolean m_useChecksum;

    private VersionsBuffer m_versionsBuffer;
    private SecondaryLogsReorgThread m_reorganizationThread;
    private SegmentHeader[] m_segmentHeaders;
    private SegmentHeader m_activeSegment;
    private SegmentHeader m_reorgSegment;
    private ReentrantLock m_segmentAssignmentlock;
    private byte[] m_reorgVector;
    private int m_segmentReorgCounter;

    private TemporaryVersionsStorage m_versionsForRecovery;

    private volatile boolean m_isAccessedByReorgThread;
    private volatile boolean m_isClosed;

    // Constructors

    /**
     * Creates an instance of SecondaryLog with default configuration except
     * secondary log size
     *
     * @param p_logComponent
     *     the log component to enable calling access granting methods in VersionsBuffer
     * @param p_reorganizationThread
     *     the reorganization thread
     * @param p_owner
     *     the NodeID
     * @param p_rangeID
     *     the RangeID
     * @param p_backupDirectory
     *     the backup directory
     * @param p_secondaryLogSize
     *     the size of a secondary log
     * @param p_flashPageSize
     *     the flash page size
     * @param p_logSegmentSize
     *     the segment size
     * @param p_reorgUtilizationThreshold
     *     the threshold size for a secondary size to trigger reorganization
     * @param p_useChecksum
     *     the logger component
     * @param p_mode
     *     the HarddriveAccessMode
     * @throws IOException
     *     if secondary log could not be created
     */
    public SecondaryLog(final LogComponent p_logComponent, final SecondaryLogsReorgThread p_reorganizationThread, final short p_owner, final short p_rangeID,
        final String p_backupDirectory, final long p_secondaryLogSize, final int p_flashPageSize, final int p_logSegmentSize,
        final int p_reorgUtilizationThreshold, final boolean p_useChecksum, final HarddriveAccessMode p_mode) throws IOException {
        super(new File(p_backupDirectory + 'N' + p_owner + '_' + SECLOG_PREFIX_FILENAME + p_owner + '_' + p_rangeID + SECLOG_POSTFIX_FILENAME),
            p_secondaryLogSize, SECLOG_HEADER.length, p_mode);
        if (p_secondaryLogSize < p_flashPageSize) {
            throw new IllegalArgumentException("Error: Secondary log too small");
        }

        m_secondaryLogSize = p_secondaryLogSize;
        m_logSegmentSize = p_logSegmentSize;
        m_useChecksum = p_useChecksum;

        m_segmentAssignmentlock = new ReentrantLock(false);

        m_owner = p_owner;
        m_rangeID = p_rangeID;

        m_versionsBuffer = new VersionsBuffer(p_logComponent, m_secondaryLogSize,
            p_backupDirectory + 'N' + p_owner + '_' + SECLOG_PREFIX_FILENAME + p_owner + '_' + p_rangeID + ".ver", p_mode);

        m_reorganizationThread = p_reorganizationThread;

        m_secondaryLogReorgThreshold = (int) (p_secondaryLogSize * ((double) p_reorgUtilizationThreshold / 100));
        m_segmentReorgCounter = 0;
        m_segmentHeaders = new SegmentHeader[(int) (p_secondaryLogSize / p_logSegmentSize)];
        m_reorgVector = new byte[(int) (p_secondaryLogSize / p_logSegmentSize)];

        m_isClosed = false;

        if (!createLogAndWriteHeader(SECLOG_HEADER)) {
            throw new IOException("Error: Secondary log " + p_rangeID + " could not be created");
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Initialized secondary log (%d)", m_secondaryLogSize);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Returns a list with all log entries in file wrapped in chunks
     *
     * @param p_fileName
     *     the file name of the secondary log
     * @param p_path
     *     the path of the directory the file is in
     * @param p_useChecksum
     *     whether checksums are used
     * @param p_secondaryLogSize
     *     the secondary log size
     * @param p_logSegmentSize
     *     the segment size
     * @param p_mode
     *     the harddrive access mode
     * @return ArrayList with all log entries as chunks
     * @throws IOException
     *     if the secondary log could not be read
     */
    public static DataStructure[] recoverFromFile(final String p_fileName, final String p_path, final boolean p_useChecksum, final long p_secondaryLogSize,
        final int p_logSegmentSize, final HarddriveAccessMode p_mode) throws IOException {
        short nodeID;
        int i = 0;
        int offset = 0;
        int logEntrySize;
        int payloadSize;
        int checksum = -1;
        long chunkID;
        boolean storesMigrations;
        byte[][] segments;
        byte[] payload;
        HashMap<Long, DataStructure> chunkMap;
        AbstractSecLogEntryHeader logEntryHeader;

        // TODO: See recoverFromLog(...)

        nodeID = Short.parseShort(p_fileName.split("_")[0].substring(1));
        storesMigrations = p_fileName.contains("M");

        chunkMap = new HashMap<Long, DataStructure>();

        segments = readAllSegmentsFromFile(p_path + p_fileName, p_secondaryLogSize, p_logSegmentSize, p_mode);

        // TODO: Reorganize log
        while (i < segments.length) {
            if (segments[i] != null) {
                while (offset < segments[i].length && segments[i][offset] != 0) {
                    // Determine header of next log entry
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(segments[i], offset);
                    if (storesMigrations) {
                        chunkID = logEntryHeader.getCID(segments[i], offset);
                    } else {
                        chunkID = ((long) nodeID << 48) + logEntryHeader.getCID(segments[i], offset);
                    }
                    payloadSize = logEntryHeader.getLength(segments[i], offset);
                    if (p_useChecksum) {
                        checksum = logEntryHeader.getChecksum(segments[i], offset);
                    }
                    logEntrySize = logEntryHeader.getHeaderSize(segments[i], offset) + payloadSize;

                    // Read payload and create chunk
                    if (offset + logEntrySize <= segments[i].length) {
                        // Create chunk only if log entry complete
                        payload = new byte[payloadSize];
                        System.arraycopy(segments[i], offset + logEntryHeader.getHeaderSize(segments[i], offset), payload, 0, payloadSize);
                        if (p_useChecksum && ChecksumHandler.calculateChecksumOfPayload(payload, 0, payloadSize) != checksum) {
                            // Ignore log entry
                            offset += logEntrySize;
                            continue;
                        }
                        chunkMap.put(chunkID, new DSByteArray(chunkID, payload));
                    }
                    offset += logEntrySize;
                }
            }
            offset = 0;
            i++;
        }

        return chunkMap.values().toArray(new DataStructure[chunkMap.size()]);
    }

    /**
     * Returns all segments of secondary log
     *
     * @param p_path
     *     the path of the file
     * @param p_secondaryLogSize
     *     the secondary log size
     * @param p_logSegmentSize
     *     the segment size
     * @param p_mode
     *     the harddrive access mode
     * @return all data
     * @throws IOException
     *     if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private static byte[][] readAllSegmentsFromFile(final String p_path, final long p_secondaryLogSize, final int p_logSegmentSize,
        final HarddriveAccessMode p_mode) throws IOException {
        byte[][] result;
        int numberOfSegments;

        numberOfSegments = (int) (p_secondaryLogSize / p_logSegmentSize);
        if (p_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            RandomAccessFile randomAccessFile;
            randomAccessFile = new RandomAccessFile(new File(p_path), "r");
            result = new byte[numberOfSegments][];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new byte[p_logSegmentSize];
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, randomAccessFile, (short) SECLOG_HEADER.length);
            }
            randomAccessFile.close();
        } else if (p_mode == HarddriveAccessMode.ODIRECT) {
            int fileID = JNIFileDirect.open(p_path, 1);
            if (fileID < 0) {
                throw new IOException("JNI Error: Cannot open logfile.");
            }
            // Allocate buffers for reading
            long readBufferAddr = JNIFileDirect.createBuffer(p_logSegmentSize);
            result = new byte[numberOfSegments][];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new byte[p_logSegmentSize];
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, fileID, readBufferAddr, p_logSegmentSize,
                    (short) SECLOG_HEADER.length, p_mode);
            }
            JNIFileDirect.freeBuffer(readBufferAddr);
            JNIFileDirect.close(fileID);
        } else {
            File file = new File(p_path);
            int fileID = JNIFileRaw.openLog(file.getName());
            if (fileID < 0) {
                throw new IOException("JNI Error: Cannot open logfile.");
            }
            // Allocate buffers for reading
            long readBufferAddr = JNIFileRaw.createBuffer(p_logSegmentSize);
            result = new byte[numberOfSegments][];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new byte[p_logSegmentSize];
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, fileID, readBufferAddr, p_logSegmentSize,
                    (short) SECLOG_HEADER.length, p_mode);
            }
            JNIFileRaw.freeBuffer(readBufferAddr);
            JNIFileRaw.closeLog(fileID);
        }

        return result;
    }

    // Setter

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    public final short getNodeID() {
        return m_owner;
    }

    /**
     * Returns the log size on disk
     *
     * @return the size
     */
    public final long getLogFileSize() {
        return getFileSize();
    }

    // Methods

    /**
     * Returns the versionsnl size on disk
     *
     * @return the size
     */
    public final long getVersionsFileSize() {
        return m_versionsBuffer.getFileSize();
    }

    /**
     * Returns the RangeID
     *
     * @return the RangeID
     */
    public final long getRangeID() {
        return m_rangeID;
    }

    @Override
    public long getOccupiedSpace() {
        return determineLogSize();
    }

    /**
     * Returns all segment sizes
     *
     * @return all segment sizes
     */
    public final String getSegmentDistribution() {
        String ret = "++++Distribution: | ";
        SegmentHeader header;

        for (int i = 0; i < m_segmentHeaders.length; i++) {
            header = m_segmentHeaders[i];
            if (header != null) {
                ret += i + " " + header.getUsedBytes() + ", u=" + String.format("%.2f", header.getUtilization()) + " | ";
            }
        }

        return ret;
    }

    /**
     * Returns whether this secondary log is currently accessed by reorg. thread
     *
     * @return whether this secondary log is currently accessed by reorg. thread
     */
    public final boolean isAccessed() {
        return m_isAccessedByReorgThread;
    }

    /**
     * Closes the secondary log and frees all memory
     *
     * @throws IOException
     *     if closing the files fail
     */
    @Override
    public void closeAndRemove() throws IOException {
        // Free version buffer and version log
        m_versionsBuffer.closeAndRemove();

        // Free log on hard drive
        super.closeAndRemove();

        m_isClosed = true;
    }

    /**
     * Returns the next version for ChunkID
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the next version
     */
    public final Version getNextVersion(final long p_chunkID) {
        return m_versionsBuffer.getNext(p_chunkID, m_owner);
    }

    /**
     * Invalidates a Chunk
     *
     * @param p_chunkID
     *     the ChunkID
     */
    public final void invalidateChunk(final long p_chunkID) {
        if (p_chunkID == m_owner) {
            // For normal chunks only the LocalID is registered
            m_versionsBuffer.put(ChunkID.getLocalID(p_chunkID), Version.INVALID_VERSION);
        } else {
            // For migrated/recovered chunks the whole ChunkID is registered
            m_versionsBuffer.put(p_chunkID, Version.INVALID_VERSION);
        }
    }

    @Override
    public final int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException, InterruptedException {
        int length = p_length;
        int logEntrySize;
        int rangeSize = 0;
        boolean isSignaled = false;
        SegmentHeader header;
        AbstractSecLogEntryHeader logEntryHeader;

        if (length <= 0 || length > m_secondaryLogSize) {
            throw new IllegalArgumentException("Error: Invalid data size (" + length + ')');
        }
        while (m_secondaryLogSize - determineLogSize() < length) {
            // #if LOGGER >= WARN
            LOGGER.warn("Secondary log for 0x%X is full. Initializing reorganization and awaiting execution", m_owner);
            // #endif /* LOGGER >= WARN */
            signalReorganizationAndWait();
        }

        // Change epoch
        if (m_versionsBuffer.isThresholdReached()) {
            if (!m_isAccessedByReorgThread) {
                // Write versions buffer to SSD
                if (m_versionsBuffer.flush()) {
                    for (SegmentHeader segmentHeader : m_segmentHeaders) {
                        if (segmentHeader != null) {
                            segmentHeader.beginEon();
                        }
                    }
                }
            } else {
                // Force reorganization thread to flush all versions (even though it is reorganizing this log
                // currently -> high update rate)
                signalReorganization();
                isSignaled = true;
            }
        }

        /*
         * Appending data cases:
         * 1. This secondary log is accessed by the reorganization thread:
         * a. Put data in currently active segment
         * b. No active segment or buffer too large to fit in: Create (new) "active segment" with given data
         * 2.
         * a. Buffer is large (at least 90% of segment size): Create new segment and append it
         * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
         */
        if (m_isAccessedByReorgThread) {
            // Reorganization thread is working on this secondary log -> only write in active segment
            if (m_activeSegment != null && m_activeSegment.getFreeBytes() >= length) {
                // Fill active segment
                writeToSecondaryLog(p_data, p_offset, (long) m_activeSegment.getIndex() * m_logSegmentSize + m_activeSegment.getUsedBytes(), length, true);
                m_activeSegment.updateUsedBytes(length);
                length = 0;
            } else {
                if (m_activeSegment != null) {
                    // There is not enough space in active segment to store the whole buffer -> first fill current
                    // one
                    header = m_segmentHeaders[m_activeSegment.getIndex()];
                    while (true) {
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(p_data, p_offset + rangeSize);
                        logEntrySize = logEntryHeader.getHeaderSize(p_data, p_offset + rangeSize) + logEntryHeader.getLength(p_data, p_offset + rangeSize);
                        if (logEntrySize > header.getFreeBytes() - rangeSize) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToSecondaryLog(p_data, p_offset, (long) header.getIndex() * m_logSegmentSize + header.getUsedBytes(), rangeSize, true);
                        header.updateUsedBytes(rangeSize);
                        length -= rangeSize;
                    }
                }

                // There is no active segment or the active segment is full
                length = createNewSegmentAndFill(p_data, p_offset + rangeSize, length, true);
                if (length > 0) {
                    // There is no free segment -> fill partly used segments
                    length = fillPartlyUsedSegments(p_data, p_offset + rangeSize, length, true);

                    // #if LOGGER >= ERROR
                    if (length > 0) {
                        LOGGER.error("Secondary log is full!");
                    }
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } else {
            if (m_activeSegment != null) {
                m_activeSegment = null;
            }

            if (length >= m_logSegmentSize * 0.9) {
                // Create new segment and fill it
                length = createNewSegmentAndFill(p_data, p_offset, length, false);
            }
            if (length > 0) {
                // Fill partly used segments if log iteration (remove task) is not in progress
                length = fillPartlyUsedSegments(p_data, p_offset, length, false);

                // #if LOGGER >= ERROR
                if (length > 0) {
                    LOGGER.error("Secondary log is full!");
                }
                // #endif /* LOGGER >= ERROR */
            }
        }

        if (determineLogSize() >= m_secondaryLogReorgThreshold && !isSignaled) {
            signalReorganization();
            // #if LOGGER >= INFO
            LOGGER.info("Threshold breached for secondary log of 0x%X. Initializing reorganization", m_owner);
            // #endif /* LOGGER >= INFO */
        }

        return p_length - length;
    }

    /**
     * Returns a list with all log entries wrapped in chunks
     *
     * @param p_chunkComponent
     *     the ChunkBackupComponent to store recovered chunks
     * @param p_doCRCCheck
     *     whether to check the payload or not
     * @return ChunkIDs of all recovered chunks, number of recovered chunks and bytes
     */
    public final RecoveryMetadata recoverFromLog(final ChunkBackupComponent p_chunkComponent, final boolean p_doCRCCheck) {
        int numberOfRecoveredLargeChunks = 0;
        long lowestLID;
        long timeToPut = 0;
        boolean doCRCCheck = p_doCRCCheck;
        AtomicInteger currentIndexCaller = new AtomicInteger(0);
        AtomicInteger currentIndexHelper = new AtomicInteger(m_segmentHeaders.length - 1);
        ReentrantLock largeChunkLock = new ReentrantLock(false);
        final RecoveryMetadata recoveryMetadata = new RecoveryMetadata();
        HashMap<Long, DSByteBuffer> largeChunks;

        // TODO: Guarantee that there is no more data to come

        if (determineLogSize() == 0) {
            // #if LOGGER >= INFO
            LOGGER.info("Backup range %d is empty. No need for recovery.", m_rangeID);
            // #endif /* LOGGER >= INFO */
            return null;
        }

        if (p_doCRCCheck && !m_useChecksum) {
            // #if LOGGER >= WARN
            LOGGER.warn("Unable do check for data corruption as no checksums are stored (configurable)!");
            // #endif /* LOGGER >= WARN */

            doCRCCheck = false;
        }

        Statistics statsCaller = new Statistics();
        Statistics statsHelper = new Statistics();
        long time = System.currentTimeMillis();

        // HashMap to store large Chunks in
        largeChunks = new HashMap<>();
        m_reorganizationThread.block();
        // #if LOGGER >= INFO
        LOGGER.info("Starting recovery of backup range %d", m_rangeID);
        // #endif /* LOGGER >= INFO */

        // Get all current versions
        if (m_versionsForRecovery == null) {
            m_versionsForRecovery = new TemporaryVersionsStorage(m_secondaryLogSize);
        } else {
            m_versionsForRecovery.clear();
        }
        lowestLID = m_versionsBuffer.readAll(m_versionsForRecovery, false);

        statsCaller.m_timeToReadVersionsFromDisk = System.currentTimeMillis() - time;

        // Write Chunks in parallel
        RecoveryWriterThread writerThread = p_chunkComponent.initRecoveryThread();

        // Determine ChunkID ranges in parallel
        RecoveryHelperThread helperThread =
            new RecoveryHelperThread(recoveryMetadata, largeChunks, largeChunkLock, lowestLID, currentIndexCaller, currentIndexHelper, p_doCRCCheck,
                statsHelper, p_chunkComponent);
        helperThread.setName("Recovery: Helper-Thread");
        helperThread.start();

        int cur = 0;
        while (cur < currentIndexHelper.get()) {
            if (m_segmentHeaders[cur] != null && !m_segmentHeaders[cur].isEmpty()) {
                recoverSegment(cur, m_versionsForRecovery, lowestLID, recoveryMetadata, largeChunks, largeChunkLock, p_chunkComponent, doCRCCheck, statsCaller);
            }
            cur = currentIndexCaller.incrementAndGet();
        }

        try {
            helperThread.join();
            while (!writerThread.finished()) {
                Thread.yield();
            }
            timeToPut = writerThread.getTimeToPut();
            writerThread.interrupt();
            writerThread.join();
        } catch (InterruptedException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Interrupt: Could not wait for RecoveryHelperThread/RecoveryWriterThread to finish!");
            // #endif /* LOGGER >= ERROR */
        }

        if (!largeChunks.isEmpty()) {
            numberOfRecoveredLargeChunks = p_chunkComponent.putRecoveredChunks(largeChunks.values().toArray(new DSByteBuffer[largeChunks.size()]));
        }
        m_reorganizationThread.unblock();

        // #if LOGGER >= INFO
        LOGGER.info("Recovery of backup range finished: ");
        LOGGER.info("\t Recovered %d chunks in %d ms", recoveryMetadata.getNumberOfChunks() + numberOfRecoveredLargeChunks, System.currentTimeMillis() - time);
        String ranges = "\t ChunkID ranges: ";
        for (long chunkID : recoveryMetadata.getCIDRanges()) {
            ranges += ChunkID.toHexString(chunkID) + ' ';
        }
        LOGGER.info(ranges);
        LOGGER.info("\t Read versions from array: \t\t\t\t%.2f %%", (double) (statsCaller.m_readVersionsFromArray + statsHelper.m_readVersionsFromArray) /
            (statsCaller.m_readVersionsFromArray + statsHelper.m_readVersionsFromArray + statsCaller.m_readVersionsFromHashTable +
                statsHelper.m_readVersionsFromHashTable) * 100);
        LOGGER.info("\t Time to read versions from SSD: \t\t\t %d ms", statsCaller.m_timeToReadVersionsFromDisk);
        LOGGER.info("\t Time to determine ranges: \t\t\t\t %d ms", statsHelper.m_timeToDetermineRanges);
        LOGGER.info("\t Time to read segments from SSD (sequential): \t\t %d ms",
            statsCaller.m_timeToReadSegmentsFromDisk + statsHelper.m_timeToReadSegmentsFromDisk);
        LOGGER.info("\t Time to read headers, check versions and checksums: \t %d ms", statsCaller.m_timeToCheck + statsHelper.m_timeToCheck);
        LOGGER.info("\t Time to create and put chunks in memory management: \t %d ms", timeToPut);
        // #endif /* LOGGER >= INFO */

        return recoveryMetadata;
    }

    /**
     * Returns all segments of secondary log
     *
     * @return all data
     * @throws IOException
     *     if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    public final byte[][] readAllSegments() throws IOException {
        byte[][] result;
        SegmentHeader header;
        int length;

        result = new byte[m_segmentHeaders.length][];
        for (int i = 0; i < m_segmentHeaders.length; i++) {
            header = m_segmentHeaders[i];
            if (header != null) {
                length = header.getUsedBytes();
                result[i] = new byte[length];
                readFromSecondaryLog(result[i], length, i * m_logSegmentSize, true);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "NodeID: " + m_owner + " - RangeID: " + m_rangeID + " - Written bytes: " + determineLogSize();
    }

    /**
     * Sets the access flag
     *
     * @param p_flag
     *     the new status
     */
    final void setAccessFlag(final boolean p_flag) {
        m_isAccessedByReorgThread = p_flag;

        // Helpful for debugging, but may cause null pointer exception for writer thread
        /*-if (!p_flag) {
            m_activeSegment = null;//
        }*/
    }

    /**
     * Returns true if there are segments that were not yet reorganized in this eon and the eon has exceeded half time
     *
     * @return whether this log needs to be reorganized or not
     */
    final boolean needToBeReorganized() {
        boolean ret = false;

        if (m_versionsBuffer.getEpoch() > Math.pow(2, 14)) {
            for (SegmentHeader segmentHeader : m_segmentHeaders) {
                if (segmentHeader != null && !segmentHeader.isEmpty() && !segmentHeader.wasReorganized()) {
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Reorganizes all segments
     *
     * @param p_segmentData
     *     a buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *     an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *     the lowest LID at the time the versions are read-in
     */
    final void reorganizeAll(final byte[] p_segmentData, final TemporaryVersionsStorage p_allVersions, final long p_lowestLID) {
        for (int i = 0; i < m_segmentHeaders.length; i++) {
            if (m_segmentHeaders[i] != null) {
                if (!reorganizeSegment(i, p_segmentData, p_allVersions, p_lowestLID)) {
                    // Reorganization failed because of an I/O error -> abort
                    break;
                }
            }
        }
    }

    /**
     * Reorganizes one segment by choosing the segment with best cost-benefit ratio
     *
     * @param p_segmentData
     *     a buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *     an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *     the lowest LID at the time the versions are read-in
     */
    final boolean reorganizeIteratively(final byte[] p_segmentData, final TemporaryVersionsStorage p_allVersions, final long p_lowestLID) {
        boolean ret;

        ret = reorganizeSegment(chooseSegment(), p_segmentData, p_allVersions, p_lowestLID);

        return ret;
    }

    /**
     * Gets current versions from log
     */
    final void resetReorgSegment() {
        m_reorgSegment = null;
    }

    /**
     * Gets current versions from log
     *
     * @param p_allVersions
     *     an array and hash table (for migrations) to store version numbers in
     * @return the lowest LID at the time the versions are read-in
     */
    final long getCurrentVersions(final TemporaryVersionsStorage p_allVersions) {
        Arrays.fill(m_reorgVector, (byte) 0);

        // Read versions from SSD and write back current view
        return m_versionsBuffer.readAll(p_allVersions, true);
    }

    /**
     * Returns the index of a free segment
     *
     * @return the index of a free segment
     */
    private byte getFreeSegment() {
        byte ret = -1;
        byte b = 0;

        while (b < m_segmentHeaders.length) {
            // Empty segment headers are null
            if (m_segmentHeaders[b] == null) {
                // Avoid reorganization segment
                if (m_reorgSegment == null || b != m_reorgSegment.getIndex()) {
                    ret = b;
                    break;
                }
            }
            b++;
        }

        return ret;
    }

    /**
     * Recover a segment from a normal secondary log
     *
     * @param p_segmentIndex
     *     the index of the segment
     * @param p_allVersions
     *     all versions
     * @param p_lowestLID
     *     the lowest LID at the time the versions were read-in
     * @param p_recoveryMetadata
     *     a class to bundle recovery metadata
     * @param p_largeChunks
     *     a HashMap to store large, split chunks
     * @param p_chunkComponent
     *     the ChunkBackupComponent
     * @param p_doCRCCheck
     *     whether to check the CRC checksum
     * @param p_stat
     *     timing statistics
     */
    private void recoverSegment(final int p_segmentIndex, final TemporaryVersionsStorage p_allVersions, final long p_lowestLID,
        final RecoveryMetadata p_recoveryMetadata, final HashMap<Long, DSByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock,
        final ChunkBackupComponent p_chunkComponent, final boolean p_doCRCCheck, final Statistics p_stat) {
        int headerSize;
        int readBytes = 0;
        int segmentLength;
        int payloadSize;
        int combinedSize = 0;
        long chunkID;
        long time;
        byte[] segmentData;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        try {
            time = System.currentTimeMillis();
            segmentData = new byte[m_logSegmentSize];
            segmentLength = readSegment(segmentData, p_segmentIndex);

            p_stat.m_timeToReadSegmentsFromDisk += System.currentTimeMillis() - time;

            if (segmentLength > 0) {
                int index = 0;
                int length = 100000;
                long[] chunkIDs = new long[length];
                int[] offsets = new int[length];
                int[] lengths = new int[length];

                while (readBytes < segmentLength) {
                    time = System.currentTimeMillis();
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(segmentData, readBytes);
                    headerSize = logEntryHeader.getHeaderSize(segmentData, readBytes);
                    payloadSize = logEntryHeader.getLength(segmentData, readBytes);
                    chunkID = logEntryHeader.getCID(segmentData, readBytes);
                    entryVersion = logEntryHeader.getVersion(segmentData, readBytes);

                    // Get current version
                    if (logEntryHeader.isMigrated()) {
                        currentVersion = p_allVersions.get(chunkID);
                        p_stat.m_readVersionsFromHashTable++;
                    } else {
                        currentVersion = p_allVersions.get(chunkID, p_lowestLID, p_stat);
                    }
                    if (currentVersion == null || m_versionsBuffer.getEpoch() == entryVersion.getEpoch()) {
                        // There is no entry in hashtable or element is more current -> get latest version from cache
                        // (Epoch can only be 1 greater because there is no flushing during recovery)
                        currentVersion = m_versionsBuffer.get(chunkID);
                    }

                    if (!logEntryHeader.isMigrated()) {
                        // Add creator
                        chunkID += (long) m_owner << 48;
                    }

                    if (currentVersion == null || currentVersion.getVersion() == 0) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Version unknown for chunk 0x%X!", chunkID);
                        // #endif /* LOGGER >= ERROR */
                    } else if (currentVersion.isEqual(entryVersion)) {
                        // Compare current version with element
                        // Create chunk only if log entry complete
                        if (p_doCRCCheck) {
                            if (ChecksumHandler.calculateChecksumOfPayload(segmentData, readBytes + headerSize, payloadSize) !=
                                logEntryHeader.getChecksum(segmentData, readBytes)) {
                                // #if LOGGER >= ERROR
                                LOGGER.error("Corrupt data. Could not recover 0x%X!", chunkID);
                                // #endif /* LOGGER >= ERROR */

                                readBytes += headerSize + payloadSize;
                                continue;
                            }
                        }
                        p_stat.m_timeToCheck += System.currentTimeMillis() - time;

                        if (logEntryHeader.isChained(segmentData, readBytes)) {
                            byte chainID = logEntryHeader.getChainID(segmentData, readBytes);
                            byte chainSize = logEntryHeader.getChainSize(segmentData, readBytes);
                            int maxLogEntrySize = AbstractLogEntryHeader.getMaxLogEntrySize();
                            //System.out.println("Found segment " + chainID + " of chained chunk " + chunkID);

                            p_largeChunkLock.lock();
                            DSByteBuffer chunk = p_largeChunks.get(chunkID);
                            if (chunk == null) {
                                // This is the first segment for this ChunkID -> create an array large enough for holding all data for this chunk
                                if (chainID == chainSize - 1) {
                                    // This is the last chain link -> complete size is known
                                    chunk = new DSByteBuffer(chunkID, (chainSize - 1) * maxLogEntrySize + payloadSize);
                                    p_largeChunks.put(chunkID, chunk);
                                } else {
                                    // This is another chain link -> maximum size is known, only -> must be truncated later
                                    chunk = new DSByteBuffer(chunkID, chainSize * maxLogEntrySize);
                                    p_largeChunks.put(chunkID, chunk);
                                }
                            }

                            ByteBuffer buffer = chunk.getData();
                            if (chainID == chainSize - 1 && payloadSize != maxLogEntrySize) {
                                buffer.limit((chainSize - 1) * maxLogEntrySize + payloadSize);
                            }

                            buffer.position(chainID * maxLogEntrySize);
                            buffer.put(segmentData, readBytes + headerSize, payloadSize);
                            p_largeChunkLock.unlock();

                        } else {
                            // Put chunks in memory
                            if (index < length) {
                                chunkIDs[index] = chunkID;
                                offsets[index] = readBytes + headerSize;
                                lengths[index] = payloadSize;
                                combinedSize += headerSize + payloadSize;

                                index++;
                            } else {
                                if (!p_chunkComponent.putRecoveredChunks(chunkIDs, segmentData, offsets, lengths, length)) {
                                    // #if LOGGER >= ERROR
                                    LOGGER.error("Memory management failure. Could not recover chunks!");
                                    // #endif /* LOGGER >= ERROR */
                                }

                                p_recoveryMetadata.add(length, combinedSize);
                                combinedSize = 0;
                                chunkIDs = new long[length];
                                chunkIDs[0] = chunkID;
                                offsets = new int[length];
                                offsets[0] = readBytes + headerSize;
                                lengths = new int[length];
                                lengths[0] = payloadSize;
                                index = 1;
                            }
                        }
                    } else {
                        // Version, epoch and/or eon is different -> ignore entry
                        p_stat.m_timeToCheck += System.currentTimeMillis() - time;
                    }
                    readBytes += headerSize + payloadSize;
                }

                // Put other chunks in memory
                if (index != 0) {
                    if (!p_chunkComponent.putRecoveredChunks(chunkIDs, segmentData, offsets, lengths, index)) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Memory management failure. Could not recover chunks!");
                        // #endif /* LOGGER >= ERROR */
                    }

                    p_recoveryMetadata.add(index, combinedSize);
                }
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Recovery failed(%d): ", m_rangeID, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Fills partly used segments
     *
     * @param p_data
     *     the buffer
     * @param p_offset
     *     the offset within the buffer
     * @param p_length
     *     the range length
     * @param p_isAccessed
     *     whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *     if the secondary log could not be read
     */

    private int fillPartlyUsedSegments(final byte[] p_data, final int p_offset, final int p_length, final boolean p_isAccessed) throws IOException {
        short segment;
        int offset = p_offset;
        int rangeSize;
        int logEntrySize;
        int length = p_length;
        SegmentHeader header;
        AbstractSecLogEntryHeader logEntryHeader;

        while (length > 0) {
            if (p_isAccessed) {
                m_segmentAssignmentlock.lock();
            }

            // Get the smallest used segment that has enough free space to store everything.
            // If there is no best fitting segment, choose an empty one.
            // If there is no empty one, return the segment with most free space.
            segment = getUsedSegment(length);
            header = m_segmentHeaders[segment];

            if (header == null) {
                // This segment is empty (there was no best fitting segment)
                header = new SegmentHeader(segment, length);
                m_segmentHeaders[segment] = header;

                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }

                writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize, length, p_isAccessed);
                length = 0;

                break;
            } else {
                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }

                if (length <= header.getFreeBytes()) {
                    // All data fits in this segment
                    writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), length, p_isAccessed);
                    header.updateUsedBytes(length);
                    length = 0;

                    break;
                } else {
                    // This is the largest left segment -> write as long as there is space left
                    rangeSize = 0;
                    while (true) {
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(p_data, offset + rangeSize);
                        logEntrySize = logEntryHeader.getHeaderSize(p_data, offset + rangeSize) + logEntryHeader.getLength(p_data, offset + rangeSize);
                        if (logEntrySize > header.getFreeBytes() - rangeSize) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), rangeSize, p_isAccessed);
                        header.updateUsedBytes(rangeSize);
                        length -= rangeSize;
                        offset += rangeSize;
                    }
                }
            }
        }

        return length;
    }

    /**
     * Creates a new segment and fills it
     *
     * @param p_data
     *     the buffer
     * @param p_offset
     *     the offset within the buffer
     * @param p_length
     *     the range length
     * @param p_isAccessed
     *     whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *     if the secondary log could not be read
     */
    private int createNewSegmentAndFill(final byte[] p_data, final int p_offset, final int p_length, final boolean p_isAccessed) throws IOException {
        int ret = p_length;
        short segment;
        SegmentHeader header;

        if (p_isAccessed) {
            m_segmentAssignmentlock.lock();
        }

        segment = getFreeSegment();
        if (segment != -1) {
            header = new SegmentHeader(segment, p_length);
            m_segmentHeaders[segment] = header;

            if (p_isAccessed) {
                // Set active segment. Must be synchronized.
                m_activeSegment = header;
                m_segmentAssignmentlock.unlock();
            }

            writeToSecondaryLog(p_data, p_offset, (long) segment * m_logSegmentSize, p_length, p_isAccessed);
            ret = 0;
        } else {
            if (p_isAccessed) {
                m_segmentAssignmentlock.unlock();
            }
        }

        return ret;
    }

    /**
     * Returns the sum of all segment sizes
     *
     * @return the sum of all segment sizes
     */
    private int determineLogSize() {
        int ret = 0;

        for (int i = 0; i < m_segmentHeaders.length; i++) {
            if (m_segmentHeaders[i] != null) {
                ret += m_segmentHeaders[i].getUsedBytes();
            }
        }

        return ret;
    }

    /**
     * Returns the index of the best-fitting segment
     *
     * @param p_length
     *     the length of the data
     * @return the index of the best-fitting segment
     */
    private short getUsedSegment(final int p_length) {
        short ret;
        short bestFitSegment = -1;
        short maxSegment = -1;
        short emptySegment = -1;
        int bestFit = Integer.MAX_VALUE;
        int max = 0;
        int freeBytes;

        for (short index = 0; index < m_segmentHeaders.length; index++) {
            if (m_segmentHeaders[index] == null) {
                // This is an empty segment. We need it if there is no best fit.
                if (emptySegment == -1) {
                    emptySegment = index;
                }
            } else {
                // Avoid reorganization segment
                if (!m_segmentHeaders[index].equals(m_reorgSegment)) {
                    freeBytes = m_segmentHeaders[index].getFreeBytes();
                    if (freeBytes >= p_length) {
                        if (freeBytes < bestFit) {
                            // In current segment is more space than needed but less than in every segment before ->
                            // current best fit
                            bestFit = freeBytes;
                            bestFitSegment = index;
                        }
                    } else if (freeBytes > max) {
                        // In current segment is less space than needed but more than in every segment before -> current
                        // maximum
                        max = freeBytes;
                        maxSegment = index;
                    }
                }
            }
        }

        // Choose segment with following order: 1. best fit 2. empty segment 3. max space
        if (bestFitSegment != -1) {
            ret = bestFitSegment;
        } else if (emptySegment != -1) {
            ret = emptySegment;
        } else {
            ret = maxSegment;
        }

        return ret;
    }

    /**
     * Returns given segment of secondary log
     *
     * @param p_data
     *     the buffer to read data into
     * @param p_segmentIndex
     *     the segment
     * @return the segment's data
     * @throws IOException
     *     if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private int readSegment(final byte[] p_data, final int p_segmentIndex) throws IOException {
        int ret = 0;
        SegmentHeader header;

        header = m_segmentHeaders[p_segmentIndex];
        if (header != null) {
            ret = header.getUsedBytes();
            readFromSecondaryLog(p_data, ret, p_segmentIndex * m_logSegmentSize, true);
        }
        return ret;
    }

    /**
     * Updates log segment
     *
     * @param p_buffer
     *     the buffer
     * @param p_length
     *     the segment length
     * @param p_segmentIndex
     *     the segment index
     * @throws IOException
     *     if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private void updateSegment(final byte[] p_buffer, final int p_length, final int p_segmentIndex) throws IOException {
        SegmentHeader header;

        // Mark the end of the segment (a log entry header cannot start with a zero)
        p_buffer[p_length] = 0;

        // Overwrite segment on log
        writeToSecondaryLog(p_buffer, 0, (long) p_segmentIndex * m_logSegmentSize, p_length + 1, true);

        // Update segment header
        header = m_segmentHeaders[p_segmentIndex];
        header.reset();
        header.updateUsedBytes(p_length);
        header.markSegmentAsReorganized();
    }

    /**
     * Frees segment
     *
     * @param p_segmentIndex
     *     the segment
     * @throws IOException
     *     if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private void freeSegment(final int p_segmentIndex) throws IOException {

        // Mark the end of the segment (a log entry header cannot start with a zero)
        writeToSecondaryLog(new byte[] {0}, 0, (long) p_segmentIndex * m_logSegmentSize, 1, true);
        m_segmentHeaders[p_segmentIndex] = null;
    }

    /**
     * Wakes up the reorganization thread
     *
     * @throws InterruptedException
     *     if caller is interrupted
     */
    private void signalReorganization() throws InterruptedException {
        m_reorganizationThread.setLogToReorgImmediately(this, false);
    }

    /**
     * Wakes up the reorganization thread and waits until reorganization is
     * finished
     *
     * @throws InterruptedException
     *     if caller is interrupted
     */
    private void signalReorganizationAndWait() throws InterruptedException {
        m_reorganizationThread.setLogToReorgImmediately(this, true);
    }

    /**
     * Reorganizes one given segment of a normal secondary log
     *
     * @param p_segmentIndex
     *     the segments index
     * @param p_segmentData
     *     a buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *     a hash table and int array with all versions for this secondary log
     * @param p_lowestLID
     *     the lowest LID at the time the versions were read-in
     * @return whether the reorganization was successful or not
     */
    private boolean reorganizeSegment(final int p_segmentIndex, final byte[] p_segmentData, final TemporaryVersionsStorage p_allVersions,
        final long p_lowestLID) {
        boolean ret = true;
        int length;
        int readBytes = 0;
        int writtenBytes = 0;
        int segmentLength;
        long chunkID;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        if (p_segmentIndex != -1 && p_allVersions != null) {
            m_segmentAssignmentlock.lock();
            if (m_activeSegment == null || m_activeSegment.getIndex() != p_segmentIndex) {
                m_reorgSegment = m_segmentHeaders[p_segmentIndex];
                m_segmentAssignmentlock.unlock();
                try {
                    segmentLength = readSegment(p_segmentData, p_segmentIndex);
                    if (segmentLength > 0) {
                        while (readBytes < segmentLength) {
                            logEntryHeader = AbstractSecLogEntryHeader.getHeader(p_segmentData, readBytes);
                            length = logEntryHeader.getHeaderSize(p_segmentData, readBytes) + logEntryHeader.getLength(p_segmentData, readBytes);
                            chunkID = logEntryHeader.getCID(p_segmentData, readBytes);
                            entryVersion = logEntryHeader.getVersion(p_segmentData, readBytes);

                            // Get current version
                            if (logEntryHeader.isMigrated()) {
                                currentVersion = p_allVersions.get(chunkID);
                            } else {
                                currentVersion = p_allVersions.get(chunkID, p_lowestLID);
                            }
                            if (currentVersion == null || m_versionsBuffer.getEpoch() == entryVersion.getEpoch()) {
                                // There is no entry in hashtable or element is more current -> get latest version from cache
                                // (Epoch can only be 1 greater because there is no flushing during reorganization)
                                currentVersion = m_versionsBuffer.get(chunkID);
                            }

                            if (currentVersion == null || currentVersion.getVersion() == 0) {
                                // #if LOGGER >= ERROR
                                LOGGER.error("Version unknown for chunk 0x%X!", chunkID);
                                // #endif /* LOGGER >= ERROR */
                            } else if (currentVersion.isEqual(entryVersion)) {
                                // Compare current version with element
                                if (readBytes != writtenBytes) {
                                    System.arraycopy(p_segmentData, readBytes, p_segmentData, writtenBytes, length);
                                }
                                writtenBytes += length;

                                if (currentVersion.getEon() != m_versionsBuffer.getEon()) {
                                    // Update eon in both versions
                                    logEntryHeader.flipEon(p_segmentData, writtenBytes - length);

                                    // Add to version buffer; all entries will get current eon during flushing
                                    m_versionsBuffer.tryPut(chunkID, currentVersion.getVersion());
                                }
                            } else {
                                // Version, epoch and/or eon is different -> remove entry
                            }
                            readBytes += length;
                        }
                        if (writtenBytes < readBytes) {
                            if (writtenBytes > 0) {
                                updateSegment(p_segmentData, writtenBytes, p_segmentIndex);
                            } else {
                                freeSegment(p_segmentIndex);
                            }
                        }
                    }
                } catch (final IOException e) {
                    if (m_isClosed) {
                        return false;
                    }
                    // #if LOGGER >= WARN
                    LOGGER.warn("Reorganization failed(log: %d): ", m_rangeID, e);
                    // #endif /* LOGGER >= WARN */
                    ret = false;
                }
            } else {
                m_segmentAssignmentlock.unlock();
            }

            if (readBytes - writtenBytes > 0) {
                // #if LOGGER >= INFO
                LOGGER
                    .info("Freed %d bytes during reorganization of segment %d in range 0x%X,%d\t total log size: %d", readBytes - writtenBytes, p_segmentIndex,
                        m_owner, m_rangeID, determineLogSize() / 1024 / 1024);
                // #endif /* LOGGER >= INFO */
            }
        }

        return ret;
    }

    /**
     * Determines the next segment to reorganize
     *
     * @return the chosen segment
     */
    private int chooseSegment() {
        int ret = -1;
        int tries;
        long costBenefitRatio;
        long max = -1;
        SegmentHeader currentSegment;

        /*
         * Choose a segment regarding the cost-benefit formula (the utilization does not contain an invalid counter)
         * Every tenth segment is chosen randomly out of all segments that have not been reorganized in this eon.
         * Avoid segments that already have been reorganized within this epoch (-> m_reorgVector).
         */
        if (m_segmentReorgCounter++ == 10) {
            tries = (int) (m_secondaryLogSize / m_logSegmentSize * 2);
            while (true) {
                ret = RandomUtils.getRandomValue((int) (m_secondaryLogSize / m_logSegmentSize) - 1);
                if (m_segmentHeaders[ret] != null && !m_segmentHeaders[ret].wasReorganized() && m_reorgVector[ret] == 0 || --tries == 0) {
                    break;
                }
            }
            m_segmentReorgCounter = 0;
        }

        if (ret == -1 || m_segmentHeaders[ret] == null) {
            // Original cost-benefit ratio: ((1-u)*age)/(1+u)
            for (int i = 0; i < m_segmentHeaders.length; i++) {
                currentSegment = m_segmentHeaders[i];
                if (currentSegment != null && m_reorgVector[i] == 0) {
                    costBenefitRatio = currentSegment.getAge();
                    if (costBenefitRatio > max) {
                        max = costBenefitRatio;
                        ret = i;
                    }
                }
            }
        }

        if (ret != -1) {
            // Mark segment as being reorganized in this epoch
            m_reorgVector[ret] = 1;
        }

        return ret;
    }

    // Classes

    /**
     * Helper class to store and print statistics for recovery
     */
    static final class Statistics {

        // Attributes
        long m_timeToReadVersionsFromDisk;
        long m_timeToDetermineRanges;
        long m_timeToReadSegmentsFromDisk;
        long m_timeToCheck;
        int m_readVersionsFromArray;
        int m_readVersionsFromHashTable;
        int m_recoveredLargeChunks;

        /**
         * Constructor
         */
        private Statistics() {
            m_timeToReadVersionsFromDisk = 0;
            m_timeToDetermineRanges = 0;
            m_timeToReadSegmentsFromDisk = 0;
            m_timeToCheck = 0;
            m_readVersionsFromArray = 0;
            m_readVersionsFromHashTable = 0;
            m_recoveredLargeChunks = 0;
        }
    }

    /**
     * SegmentHeader
     *
     * @author Kevin Beineke 07.11.2014
     */
    private final class SegmentHeader {

        // Attributes
        private int m_index;
        private int m_usedBytes;
        private long m_lastAccess;
        private boolean m_reorgInCurrEon;

        // Constructors

        /**
         * Creates an instance of SegmentHeader
         *
         * @param p_usedBytes
         *     the number of used bytes
         * @param p_index
         *     the index within the log
         */
        private SegmentHeader(final int p_index, final int p_usedBytes) {
            m_index = p_index;
            m_usedBytes = p_usedBytes;
            m_lastAccess = System.currentTimeMillis();
            m_reorgInCurrEon = true;
        }

        // Getter

        /**
         * Returns the utilization
         *
         * @return the utilization
         */
        private float getUtilization() {
            return 1 - (float) m_usedBytes / m_logSegmentSize;
        }

        /**
         * Returns the index
         *
         * @return the index
         */
        private int getIndex() {
            return m_index;
        }

        /**
         * Returns whether this segment is empty or not
         *
         * @return true if segment is empty, false otherwise
         */
        private boolean isEmpty() {
            return m_usedBytes == 0;
        }

        /**
         * Returns number of used bytes
         *
         * @return number of used bytes
         */
        private int getUsedBytes() {
            return m_usedBytes;
        }

        /**
         * Returns number of used bytes
         *
         * @return number of used bytes
         */
        private int getFreeBytes() {
            return m_logSegmentSize - m_usedBytes;
        }

        /**
         * Returns the age of this segment
         *
         * @return the age of this segment
         */
        private long getAge() {
            return System.currentTimeMillis() - m_lastAccess;
        }

        /**
         * Returns whether this segment was reorganized in current eon
         *
         * @return whether this segment was reorganized in current eon
         */
        private boolean wasReorganized() {
            return m_reorgInCurrEon;
        }

        // Setter

        /**
         * Updates the number of used bytes
         *
         * @param p_writtenBytes
         *     the number of written bytes
         */
        private void updateUsedBytes(final int p_writtenBytes) {
            m_usedBytes += p_writtenBytes;
            m_lastAccess = System.currentTimeMillis();
        }

        /**
         * Sets the reorganization status for current eon
         */
        private void markSegmentAsReorganized() {
            m_reorgInCurrEon = true;
        }

        /**
         * Resets the reorganization status for new eon
         */
        private void beginEon() {
            m_reorgInCurrEon = false;
        }

        /**
         * Resets the segment header
         */
        private void reset() {
            m_usedBytes = 0;
            m_lastAccess = System.currentTimeMillis();
        }
    }

    /**
     * Recovery helper thread. Determines ChunkID ranges for to be recovered backup range and recovers segments as well.
     */
    private class RecoveryHelperThread extends Thread {

        private RecoveryMetadata m_recoveryMetadata;
        private HashMap<Long, DSByteBuffer> m_largeChunks;
        private long m_lowestLID;
        private AtomicInteger m_currentIndexCaller;
        private AtomicInteger m_currentIndexHelper;
        private ReentrantLock m_largeChunkLock;
        private boolean m_doCRCCheck;
        private Statistics m_stats;
        private ChunkBackupComponent m_chunkComponent;

        RecoveryHelperThread(final RecoveryMetadata p_metadata, final HashMap<Long, DSByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock,
            final long p_lowestLID, final AtomicInteger p_currentIndexCaller, final AtomicInteger p_currentIndexHelper, final boolean p_doCRCCheck,
            final Statistics p_stat, final ChunkBackupComponent p_chunkComponent) {
            m_recoveryMetadata = p_metadata;
            m_largeChunks = p_largeChunks;
            m_lowestLID = p_lowestLID;
            m_currentIndexCaller = p_currentIndexCaller;
            m_currentIndexHelper = p_currentIndexHelper;
            m_largeChunkLock = p_largeChunkLock;
            m_doCRCCheck = p_doCRCCheck;
            m_stats = p_stat;
            m_chunkComponent = p_chunkComponent;
        }

        @Override
        public void run() {
            long t = System.currentTimeMillis();
            m_recoveryMetadata.setChunkIDRanges(determineRanges(m_versionsForRecovery, m_owner, m_lowestLID));
            m_stats.m_timeToDetermineRanges = System.currentTimeMillis() - t;

            int cur = m_segmentHeaders.length - 1;
            while (cur > m_currentIndexCaller.get()) {
                if (m_segmentHeaders[cur] != null && !m_segmentHeaders[cur].isEmpty()) {
                    recoverSegment(cur, m_versionsForRecovery, m_lowestLID, m_recoveryMetadata, m_largeChunks, m_largeChunkLock, m_chunkComponent, m_doCRCCheck,
                        m_stats);
                }
                cur = m_currentIndexHelper.decrementAndGet();
            }
        }

        /**
         * Determines ChunkID ranges for recovery
         *
         * @param p_versionStorage
         *     all versions in array and hashtable
         * @param p_owner
         *     the owner of all chunks
         * @param p_lowestLID
         *     the lowest ChunkID in versions array
         * @return all ChunkID ranges
         */
        private long[] determineRanges(final TemporaryVersionsStorage p_versionStorage, final short p_owner, final long p_lowestLID) {
            long[] ret;
            long[] localRanges = null;
            long[] otherRanges = null;

            if (p_versionStorage.getVersionsArray().size() > 0) {
                localRanges = getRanges(p_versionStorage.getVersionsArray(), p_owner, p_lowestLID);
            }
            if (p_versionStorage.getVersionsHashTable().size() > 0) {
                otherRanges = getRanges(p_versionStorage.getVersionsHashTable());
            }

            if (localRanges == null) {
                ret = otherRanges;
            } else if (otherRanges == null) {
                ret = localRanges;
            } else {
                ret = new long[localRanges.length + otherRanges.length];
                System.arraycopy(localRanges, 0, ret, 0, localRanges.length);
                System.arraycopy(otherRanges, 0, ret, localRanges.length, otherRanges.length);
            }

            return ret;
        }

        /**
         * Determines all ChunkID ranges in versions array
         *
         * @param p_versionsArray
         *     the versions array
         * @param p_creator
         *     the creator and current owner
         * @return all ChunkID ranges in versions array
         */
        private long[] getRanges(final VersionsArray p_versionsArray, final short p_creator, final long p_lowestLID) {
            int currentIndex;
            int index = 0;
            long currentLID;
            ArrayListLong ranges = new ArrayListLong();

            while (index < p_versionsArray.capacity()) {
                if (p_versionsArray.getVersion(index, 0) == Version.INVALID_VERSION) {
                    index++;
                    continue;
                }

                currentLID = index + p_lowestLID;
                ranges.add(((long) p_creator << 48) + currentLID);
                currentIndex = 1;

                while (index + currentIndex < p_versionsArray.capacity() && p_versionsArray.getVersion(index + currentIndex, 0) != Version.INVALID_VERSION) {
                    currentIndex++;
                }
                ranges.add(((long) p_creator << 48) + currentLID + currentIndex - 1);
                index += currentIndex;
            }

            return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
        }

        /**
         * Determines all ChunkID ranges in versions hashtable
         *
         * @param p_versionsHT
         *     the versions hashtable
         * @return all ChunkID ranges in versions hashtable
         */
        private long[] getRanges(final VersionsHashTable p_versionsHT) {
            int currentIndex;
            int index = 0;
            long currentCID;
            int[] table = p_versionsHT.getTable();
            ArrayListLong ranges = new ArrayListLong();

            // Sort table
            if (p_versionsHT.size() < SORT_THRESHOLD) {
                // There are only a few elements in table -> for a nearly sorted table insertion sort is much faster than quicksort
                insertionSort(table);
            } else {
                quickSort(table, 0, table.length - 1);
            }

            while (index < table.length) {
                if (table[index] == 0) {
                    index += 4;
                    continue;
                }

                currentCID = (long) table[index] << 32 | table[index + 1] & 0xFFFFFFFFL;
                ranges.add(currentCID);
                currentIndex = 4;

                while (index + currentIndex < table.length &&
                    ((long) table[index + currentIndex] << 32 | table[index + currentIndex + 1] & 0xFFFFFFFFL) == currentCID + currentIndex / 4) {
                    currentIndex += 4;
                }
                ranges.add(currentCID + currentIndex - 1);
                index += currentIndex;
            }

            return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
        }

        /**
         * Sorts the versions hashtable with insertion sort; Used for a barely utilized hashtable as insertion sort is best for nearly sorted series
         *
         * @param p_table
         *     the array of the versions hashtable
         */
        private void insertionSort(int[] p_table) {
            for (int i = 0; i < p_table.length; i += 4) {
                for (int j = i; j > 0 && p_table[j] < p_table[j - 4]; j -= 4) {
                    swap(p_table, j, j - 4);
                }
            }
        }

        /**
         * Sorts the versions hashtable with quicksort; Used for highly a utilized hashtable
         *
         * @param p_table
         *     the array of the versions hashtable
         * @param p_pivotIndex
         *     the index of the pivot element
         * @param p_rangeIndex
         *     the range index
         */
        private void quickSort(int[] p_table, int p_pivotIndex, int p_rangeIndex) {
            if (p_pivotIndex < p_rangeIndex) {
                int q = partition(p_table, p_pivotIndex, p_rangeIndex);
                quickSort(p_table, p_pivotIndex, q);
                quickSort(p_table, q + 4, p_rangeIndex);
            }
        }

        /**
         * Helper method for quicksort to partition the range
         *
         * @param p_table
         *     the array of the versions hashtable
         * @param p_pivotIndex
         *     the index of the pivot element
         * @param p_rangeIndex
         *     the range index
         * @return the partition index
         */
        private int partition(int[] p_table, int p_pivotIndex, int p_rangeIndex) {

            int cmpIndex = p_table[p_pivotIndex];
            int i = p_pivotIndex - 4;
            int j = p_rangeIndex + 4;

            while (true) {
                i += 4;
                while (i < p_rangeIndex && p_table[i] < cmpIndex) {
                    i += 4;
                }

                j -= 4;
                while (j > p_pivotIndex && p_table[j] > cmpIndex) {
                    j -= 4;
                }

                if (i < j) {
                    swap(p_table, i, j);
                } else {
                    return j;
                }
            }
        }

        /**
         * Helper method for quicksort and insertion sort to swap to elements
         *
         * @param p_table
         *     the array of the versions hashtable
         * @param p_index1
         *     the first index
         * @param p_index2
         *     the second index
         */
        private void swap(int[] p_table, int p_index1, int p_index2) {
            if (p_table[p_index1] == 0) {
                if (p_table[p_index2] != 0) {
                    p_table[p_index1] = p_table[p_index2];
                    p_table[p_index1 + 1] = p_table[p_index2 + 1];
                    p_table[p_index1 + 2] = p_table[p_index2 + 2];
                    p_table[p_index1 + 3] = p_table[p_index2 + 3];

                    p_table[p_index2] = 0;
                    p_table[p_index2 + 1] = 0;
                    p_table[p_index2 + 2] = 0;
                    p_table[p_index2 + 3] = 0;
                }
            } else if (p_table[p_index2] == 0) {
                p_table[p_index2] = p_table[p_index1];
                p_table[p_index2 + 1] = p_table[p_index1 + 1];
                p_table[p_index2 + 2] = p_table[p_index1 + 2];
                p_table[p_index2 + 3] = p_table[p_index2 + 3];

                p_table[p_index1] = 0;
                p_table[p_index1 + 1] = 0;
                p_table[p_index1 + 2] = 0;
                p_table[p_index1 + 3] = 0;
            } else {
                int temp1 = p_table[p_index1];
                int temp2 = p_table[p_index1 + 1];
                int temp3 = p_table[p_index1 + 2];
                int temp4 = p_table[p_index1 + 3];

                p_table[p_index1] = p_table[p_index2];
                p_table[p_index1 + 1] = p_table[p_index2 + 1];
                p_table[p_index1 + 2] = p_table[p_index2 + 2];
                p_table[p_index1 + 3] = p_table[p_index2 + 3];

                p_table[p_index2] = temp1;
                p_table[p_index2 + 1] = temp2;
                p_table[p_index2 + 2] = temp3;
                p_table[p_index2 + 3] = temp4;
            }
        }
    }
}
