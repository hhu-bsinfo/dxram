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

package de.hhu.bsinfo.dxram.log.storage.logs.secondarylog;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.logs.Log;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * This class implements the secondary log
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.10.2014
 */
public final class SecondaryLog extends Log {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLog.class.getSimpleName());

    private static final TimePool SOP_WRITE_SECONDARY_LOG = new TimePool(SecondaryLog.class, "WriteSecondaryLog");
    private static final TimePool SOP_READ_SECONDARY_LOG = new TimePool(SecondaryLog.class, "ReadSecondaryLog");
    private static final ValuePool SOP_WRITE_SECONDARY_LOG_DATA =
            new ValuePool(SecondaryLog.class, "WriteSecondaryLogSize");

    private static DirectByteBufferWrapper ms_nullSegmentWrapper;

    static {
        StatisticsManager.get().registerOperation(SecondaryLog.class, SOP_WRITE_SECONDARY_LOG);
        StatisticsManager.get().registerOperation(SecondaryLog.class, SOP_READ_SECONDARY_LOG);
        StatisticsManager.get().registerOperation(SecondaryLog.class, SOP_WRITE_SECONDARY_LOG_DATA);
    }

    private final Scheduler m_scheduler;

    private final short m_originalOwner;
    private final short m_originalRangeID;
    private final long m_secondaryLogReorgThreshold;
    private final long m_secondaryLogSize;
    private final int m_logSegmentSize;
    private final boolean m_useTimestamps;
    private final long m_initializationTimestamp;

    private final VersionBuffer m_versionBuffer;
    private final SegmentHeader[] m_segmentHeaders;
    private final ReentrantLock m_segmentAssignmentlock;
    private final ReentrantLock m_fileAccessLock;
    private final BitSet m_reorgVector;

    private short m_owner;
    private short m_rangeID;

    private SegmentHeader m_activeSegment;
    private SegmentHeader m_reorgSegment;

    private volatile boolean m_isAccessedByReorgThread;

    /**
     * Creates an instance of SecondaryLog with default configuration except
     * secondary log size
     *
     * @param p_versionBuffer
     *         the version buffer
     * @param p_owner
     *         the NodeID
     * @param p_rangeID
     *         the RangeID
     * @param p_secondaryLogSize
     *         the size of a secondary log
     * @param p_flashPageSize
     *         the flash page size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_reorgUtilizationThreshold
     *         the threshold size for a secondary size to trigger reorganization
     * @param p_useTimestamps
     *         whether timestamps are used for segment selection
     * @param p_initializationTimestamp
     *         time of initialization of the logging component. Used for relative age of log entries
     * @param p_fileName
     *         the file name (including backup directory) for the log
     * @throws IOException
     *         if secondary log could not be created
     */
    SecondaryLog(final Scheduler p_scheduler, final VersionBuffer p_versionBuffer, final short p_owner,
            final short p_originalOwner, final short p_rangeID, final long p_secondaryLogSize,
            final int p_flashPageSize, final int p_logSegmentSize, final int p_reorgUtilizationThreshold,
            final boolean p_useTimestamps, final long p_initializationTimestamp, final String p_fileName)
            throws IOException {
        super(new File(p_fileName), p_secondaryLogSize);

        if (p_secondaryLogSize < p_flashPageSize) {
            throw new IllegalArgumentException("Error: Secondary log too small");
        }

        m_scheduler = p_scheduler;

        m_secondaryLogSize = p_secondaryLogSize;
        m_logSegmentSize = p_logSegmentSize;
        m_useTimestamps = p_useTimestamps;
        m_initializationTimestamp = p_initializationTimestamp;

        m_segmentAssignmentlock = new ReentrantLock(false);

        m_owner = p_owner;
        m_rangeID = p_rangeID;
        m_originalOwner = p_originalOwner;
        m_originalRangeID = p_rangeID;

        m_versionBuffer = p_versionBuffer;

        m_secondaryLogReorgThreshold = (int) (p_secondaryLogSize * ((double) p_reorgUtilizationThreshold / 100));
        m_segmentHeaders = new SegmentHeader[(int) (p_secondaryLogSize / p_logSegmentSize)];
        m_reorgVector = new BitSet((int) (p_secondaryLogSize / p_logSegmentSize));

        m_fileAccessLock = new ReentrantLock(false);
        ms_nullSegmentWrapper = new DirectByteBufferWrapper(p_flashPageSize, true);

        try {
            createLog();
        } catch (final IOException e) {
            throw new IOException("Error: Secondary log " + p_rangeID + " could not be created");
        }

        LOGGER.trace("Initialized secondary log (%d)", m_secondaryLogSize);

    }

    @Override
    public long getOccupiedSpace() {
        return determineLogSize();
    }

    @Override
    public void readFromLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_length, final long p_readPos)
            throws IOException {
        final long bytesUntilEnd = m_logSize - p_readPos;

        if (p_length > 0) {
            SOP_READ_SECONDARY_LOG.start();

            // All reads might be concurrent to writes by writer thread -> lock file
            m_fileAccessLock.lock();

            assert p_length <= bytesUntilEnd;
            assert p_readPos % m_logSegmentSize + p_length <= m_logSegmentSize;

            ms_logAccess.read(m_log, p_bufferWrapper, p_length, p_readPos);

            m_fileAccessLock.unlock();

            SOP_READ_SECONDARY_LOG.stop();
        }
    }

    @Override
    public final void writeToLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_accessed) throws IOException {

        if (p_length > 0) {
            SOP_WRITE_SECONDARY_LOG_DATA.add(p_length);
            SOP_WRITE_SECONDARY_LOG.start();

            if (p_accessed) {
                m_fileAccessLock.lock();
            }

            assert p_writePos + p_length <= m_logSize;
            assert p_writePos % m_logSegmentSize + p_length <= m_logSegmentSize;

            if (p_bufferWrapper == null && p_length == 1) {
                // Write 0 to the beginning of the segment
                ms_logAccess.write(m_log, ms_nullSegmentWrapper, 0, p_writePos, 1, false);
            } else {
                if (p_bufferWrapper == null) {
                    throw new IOException("Error writing to log. Buffer wrapper is null");
                }

                if (p_bufferOffset + p_length + 1 < p_bufferWrapper.getBuffer().capacity() &&
                        p_writePos % m_logSegmentSize + p_length < m_logSegmentSize) {

                    // Mark the end of the segment
                    byte oldByte = p_bufferWrapper.getBuffer().get(p_bufferOffset + p_length);
                    p_bufferWrapper.getBuffer().put(p_bufferOffset + p_length, (byte) 0);

                    ms_logAccess.write(m_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length + 1, false);

                    // Write back old byte at boundary
                    p_bufferWrapper.getBuffer().put(p_bufferOffset + p_length, oldByte);
                } else {
                    ms_logAccess.write(m_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length, false);

                    if (p_writePos % m_logSegmentSize + p_length < m_logSegmentSize) {
                        // Mark end of the segment
                        ms_logAccess.write(m_log, ms_nullSegmentWrapper, 0, p_writePos + p_length, 1, false);
                    }
                }
            }

            if (p_accessed) {
                m_fileAccessLock.unlock();
            }

            SOP_WRITE_SECONDARY_LOG.stop();
        }
    }

    /**
     * Writes data to secondary log. The given buffer might be allocated to different segments if there is not enough
     * space.
     *
     * @param p_bufferWrapper
     *         the buffer containing the data
     * @param p_length
     *         the write size
     * @throws IOException
     *         if the data could not be written to disk
     */
    final void postData(final DirectByteBufferWrapper p_bufferWrapper, final int p_length) throws IOException {
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

            LOGGER.warn(
                    "Secondary log for range %d of 0x%X is full. Initializing reorganization and awaiting execution",
                    m_rangeID, m_owner);

            signalReorganizationAndWait();
        }

        // Change epoch
        if (m_versionBuffer.isThresholdReached()) {
            if (!m_isAccessedByReorgThread) {
                // Write versions buffer to SSD
                if (m_versionBuffer.flush()) {
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
         * a. Buffer is large (at least 75% of segment size): Create new segment and append it
         * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
         */
        if (m_isAccessedByReorgThread) {
            // Reorganization thread is working on this secondary log -> only write in active segment
            if (m_activeSegment != null && length <= m_activeSegment.getFreeBytes(m_logSegmentSize)) {
                // Fill active segment
                writeToLog(p_bufferWrapper, 0,
                        (long) m_activeSegment.getIndex() * m_logSegmentSize + m_activeSegment.getUsedBytes(), length,
                        true);
                m_activeSegment.updateUsedBytes(length, getCurrentTimeInSec());
                if (m_useTimestamps) {
                    // Modify segment age
                    int currentAge = m_activeSegment.getAge(getCurrentTimeInSec());
                    m_activeSegment.setAge(currentAge -
                            (currentAge + getCurrentTimeInSec() - m_activeSegment.getLastAccess()) * length /
                                    m_activeSegment.getUsedBytes() /* contains length already */);
                }
            } else {
                if (m_activeSegment != null) {
                    // There is not enough space in active segment to store the whole buffer -> first fill current one
                    header = m_segmentHeaders[m_activeSegment.getIndex()];
                    while (true) {
                        short type = (short) (p_bufferWrapper.getBuffer().get(rangeSize) & 0xFF);
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(type);
                        logEntrySize = logEntryHeader.getHeaderSize(type) +
                                logEntryHeader.getLength(type, p_bufferWrapper.getBuffer(), rangeSize);
                        if (rangeSize + logEntrySize > header.getFreeBytes(m_logSegmentSize)) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToLog(p_bufferWrapper, 0,
                                (long) header.getIndex() * m_logSegmentSize + header.getUsedBytes(), rangeSize, true);
                        header.updateUsedBytes(rangeSize, getCurrentTimeInSec());
                        if (m_useTimestamps) {
                            // Modify segment age
                            int currentAge = header.getAge(getCurrentTimeInSec());
                            header.setAge(currentAge -
                                    (currentAge + getCurrentTimeInSec() - header.getLastAccess()) * rangeSize /
                                            header.getUsedBytes() /* contains rangeSize already */);
                        }
                        length -= rangeSize;
                    }
                }

                // There is no active segment or the active segment is full
                length = createNewSegmentAndFill(p_bufferWrapper, rangeSize, length, true);
                if (length > 0) {
                    // There is no free segment -> fill partly used segments
                    length = fillPartlyUsedSegments(p_bufferWrapper, rangeSize, length, true);

                    if (length > 0) {
                        LOGGER.error("Secondary log is full!");
                    }

                }
            }
        } else {
            if (m_activeSegment != null) {
                m_activeSegment = null;
            }

            if (length >= m_logSegmentSize * 0.75) {
                // Create new segment and fill it
                length = createNewSegmentAndFill(p_bufferWrapper, 0, length, false);
            }
            if (length > 0) {
                // Fill partly used segments if log iteration (remove task) is not in progress
                length = fillPartlyUsedSegments(p_bufferWrapper, 0, length, false);

                if (length > 0) {
                    LOGGER.error("Secondary log is full!");
                }

            }
        }

        if (determineLogSize() >= m_secondaryLogReorgThreshold && !isSignaled) {
            signalReorganization();

            LOGGER.trace("Threshold breached (%d) for secondary log %d of 0x%X. Initializing reorganization.",
                    determineLogSize(), m_rangeID, m_owner);

        }
    }

    /**
     * Returns the index of a free segment.
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
     * Fills partly used segments.
     *
     * @param p_bufferWrapper
     *         the buffer
     * @param p_offset
     *         the offset within the buffer
     * @param p_length
     *         the range length
     * @param p_isAccessed
     *         whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *         if the secondary log could not be read
     */
    private int fillPartlyUsedSegments(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset,
            final int p_length, final boolean p_isAccessed) throws IOException {
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
                header = new SegmentHeader(segment, length, getCurrentTimeInSec());
                m_segmentHeaders[segment] = header;

                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }
                writeToLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize, length, p_isAccessed);
                // We do not have to update the header's utilization here as the new header was initialized with
                // correct utilization
                if (m_useTimestamps) {
                    // Modify segment age
                    int currentAge = header.getAge(getCurrentTimeInSec());
                    header.setAge(currentAge - (currentAge + getCurrentTimeInSec() - header.getLastAccess()) * length /
                            header.getUsedBytes() /* contains length already */);
                }
                length = 0;

                break;
            } else {
                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }

                if (length <= header.getFreeBytes(m_logSegmentSize)) {
                    // All data fits in this segment
                    writeToLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(),
                            length, p_isAccessed);
                    header.updateUsedBytes(length, getCurrentTimeInSec());
                    if (m_useTimestamps) {
                        // Modify segment age
                        int currentAge = header.getAge(getCurrentTimeInSec());
                        header.setAge(currentAge -
                                (currentAge + getCurrentTimeInSec() - header.getLastAccess()) * length /
                                        header.getUsedBytes() /* contains length already */);
                    }
                    length = 0;

                    break;
                } else {
                    // This is the largest left segment -> write as long as there is space left
                    rangeSize = 0;
                    while (offset + rangeSize < p_offset + p_length) {
                        short type = (short) (p_bufferWrapper.getBuffer().get(offset + rangeSize) & 0xFF);
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(type);
                        logEntrySize = logEntryHeader.getHeaderSize(type) +
                                logEntryHeader.getLength(type, p_bufferWrapper.getBuffer(), offset + rangeSize);
                        if (rangeSize + logEntrySize > header.getFreeBytes(m_logSegmentSize)) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(),
                                rangeSize, p_isAccessed);
                        header.updateUsedBytes(rangeSize, getCurrentTimeInSec());
                        if (m_useTimestamps) {
                            // Modify segment age
                            int currentAge = header.getAge(getCurrentTimeInSec());
                            header.setAge(currentAge -
                                    (currentAge + getCurrentTimeInSec() - header.getLastAccess()) * rangeSize /
                                            header.getUsedBytes() /* contains rangeSize already */);
                        }
                        length -= rangeSize;
                        offset += rangeSize;
                    } else {
                        // The segment with most free space is too small to store the first log entry to write
                        // -> signal reorganization thread and wait for execution
                        LOGGER.warn("Secondary log for range %d of 0x%X is full. Cannot write log entries anymore." +
                                " Initializing reorganization and awaiting execution", m_rangeID, m_owner);

                        signalReorganizationAndWait();
                    }
                }
            }
        }

        return length;
    }

    /**
     * Creates a new segment and fills it.
     *
     * @param p_bufferWrapper
     *         the buffer
     * @param p_offset
     *         the offset within the buffer
     * @param p_length
     *         the range length
     * @param p_isAccessed
     *         whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *         if the secondary log could not be read
     */
    private int createNewSegmentAndFill(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset,
            final int p_length, final boolean p_isAccessed) throws IOException {
        int ret = p_length;
        short segment;
        SegmentHeader header;

        if (p_isAccessed) {
            m_segmentAssignmentlock.lock();
        }

        segment = getFreeSegment();
        if (segment != -1) {
            header = new SegmentHeader(segment, p_length, getCurrentTimeInSec());
            m_segmentHeaders[segment] = header;

            if (p_isAccessed) {
                // Set active segment. Must be synchronized.
                m_activeSegment = header;
                m_segmentAssignmentlock.unlock();
            }
            writeToLog(p_bufferWrapper, p_offset, (long) segment * m_logSegmentSize, p_length, p_isAccessed);
            // We do not have to update the header's utilization here as the new header was initialized with
            if (m_useTimestamps) {
                // Modify segment age
                int currentAge = header.getAge(getCurrentTimeInSec());
                header.setAge(currentAge -
                                (currentAge + getCurrentTimeInSec() - header.getLastAccess()) * p_length / header.getUsedBytes()
                        /* contains p_length already */);
            }
            ret = 0;
        } else {
            if (p_isAccessed) {
                m_segmentAssignmentlock.unlock();
            }
        }

        return ret;
    }

    /**
     * Returns the index of the best-fitting segment.
     *
     * @param p_length
     *         the length of the data
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
                    freeBytes = m_segmentHeaders[index].getFreeBytes(m_logSegmentSize);
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
     * Returns given segment of secondary log.
     *
     * @param p_bufferWrapper
     *         the aligned buffer to read data into
     * @param p_segmentIndex
     *         the segment
     * @return the segment's data
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    int readSegment(final DirectByteBufferWrapper p_bufferWrapper, final int p_segmentIndex) throws IOException {
        int ret = 0;
        SegmentHeader header;

        header = m_segmentHeaders[p_segmentIndex];
        if (header != null) {
            ret = header.getUsedBytes();
            p_bufferWrapper.getBuffer().clear();
            readFromLog(p_bufferWrapper, ret, p_segmentIndex * m_logSegmentSize);

            LOGGER.debug("Read segment %d in range 0x%X,%d: %d", p_segmentIndex, m_owner, m_rangeID,
                    m_segmentHeaders[p_segmentIndex].getUsedBytes());
        }

        return ret;
    }

    /**
     * Updates log segment.
     *
     * @param p_bufferWrapper
     *         the aligned buffer
     * @param p_length
     *         the segment length
     * @param p_segmentIndex
     *         the segment index
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    void updateSegment(final DirectByteBufferWrapper p_bufferWrapper, final int p_length, final int p_segmentIndex)
            throws IOException {
        SegmentHeader header;

        // Overwrite segment on log
        writeToLog(p_bufferWrapper, 0, (long) p_segmentIndex * m_logSegmentSize, p_length, true);

        // Update segment header
        header = m_segmentHeaders[p_segmentIndex];
        header.reset(getCurrentTimeInSec());
        header.updateUsedBytes(p_length, getCurrentTimeInSec());
        header.markSegmentAsReorganized();
    }

    /**
     * Frees segment.
     *
     * @param p_segmentIndex
     *         the segment
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    void freeSegment(final int p_segmentIndex) throws IOException {
        // Mark the end of the segment (a log entry header cannot start with a zero)
        writeToLog(null, 0, (long) p_segmentIndex * m_logSegmentSize, 1, true);
        m_segmentHeaders[p_segmentIndex] = null;
    }

    /**
     * Update owner, range ID and file name after recovery.
     *
     * @param p_restorer
     *         NodeID of the peer which recovered the backup range
     * @param p_newRangeID
     *         the new RangeID
     * @param p_newFile
     *         the new file name
     */
    void transferBackupRange(final short p_restorer, final short p_newRangeID, final String p_newFile)
            throws IOException {
        m_owner = p_restorer;
        m_rangeID = p_newRangeID;

        renameLog(new File(p_newFile));
    }

    /**
     * Returns the original owner.
     *
     * @return the NodeID
     */
    final short getOriginalOwner() {
        return m_originalOwner;
    }

    /**
     * Returns the current owner.
     *
     * @return the NodeID
     */
    public final short getOwner() {
        return m_owner;
    }

    /**
     * Returns the RangeID.
     *
     * @return the RangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Returns all segment sizes.
     *
     * @return all segment sizes
     */
    public final String getSegmentDistribution() {
        StringBuilder ret = new StringBuilder("++++Distribution: | ");
        SegmentHeader header;

        for (int i = 0; i < m_segmentHeaders.length; i++) {
            header = m_segmentHeaders[i];
            if (header != null) {
                ret.append(i).append(' ').append(header.getUsedBytes()).append(", u=")
                        .append(String.format("%.2f", header.getUtilization(m_logSegmentSize))).append(" | ");
            }
        }

        return ret.toString();
    }

    /**
     * Returns whether this secondary log is currently accessed by reorg. thread.
     *
     * @return whether this secondary log is currently accessed by reorg. thread
     */
    public final boolean isAccessed() {
        return m_isAccessedByReorgThread;
    }

    /**
     * Sets the access flag.
     *
     * @param p_flag
     *         the new status
     */
    final void setAccessFlag(final boolean p_flag) {
        m_isAccessedByReorgThread = p_flag;
    }

    @Override
    public String toString() {
        if (m_owner == m_originalOwner) {
            return "Owner: " + m_owner + " - RangeID: " + m_rangeID + " - Written bytes: " + determineLogSize();
        } else {
            return "Owner: " + m_owner + " - RangeID: " + m_rangeID + " - Original Owner: " + m_owner +
                    " - Original RangeID: " + m_originalRangeID + " - Written bytes: " + determineLogSize();
        }
    }

    /**
     * Returns true if there are segments that were not yet reorganized in this eon and the eon has exceeded half time.
     *
     * @return whether this log needs to be reorganized or not
     */
    final boolean needToBeReorganized() {
        boolean ret = false;

        if (m_versionBuffer.getEpoch() > Math.pow(2, 14)) {
            for (SegmentHeader segmentHeader : m_segmentHeaders) {
                if (segmentHeader != null && !segmentHeader.isEmpty() && segmentHeader.wasNotReorganized()) {
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Resets the current reorganization segment.
     */
    final void resetReorgSegment() {
        m_reorgSegment = null;
    }

    /**
     * Returns the sum of all segment sizes.
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
     * Returns the reorganization vector.
     *
     * @return the reorganization vector
     */
    BitSet getReorgVector() {
        return m_reorgVector;
    }

    /**
     * Returns the segment header.
     *
     * @param p_segmentIndex
     *         the index
     * @return the segment header
     */
    SegmentHeader getSegmentHeader(final int p_segmentIndex) {
        return m_segmentHeaders[p_segmentIndex];
    }

    /**
     * Returns all segment headers.
     *
     * @return all segment headers
     */
    SegmentHeader[] getSegmentHeaders() {
        return m_segmentHeaders;
    }

    /**
     * Assigns the reorganization segment.
     *
     * @param p_segmentIndex
     *         the index
     * @return whether the reorganization segment was assigned or not
     */
    boolean assignReorgSegment(final int p_segmentIndex) {
        boolean ret = false;

        m_segmentAssignmentlock.lock();
        if (m_activeSegment == null || m_activeSegment.getIndex() != p_segmentIndex) {
            m_reorgSegment = m_segmentHeaders[p_segmentIndex];
            ret = true;
        }
        m_segmentAssignmentlock.unlock();

        return ret;
    }

    /**
     * Returns the time since log creation in seconds (overflow occurs after 68+ years).
     *
     * @return the current time in seconds
     */
    int getCurrentTimeInSec() {
        return (int) ((System.currentTimeMillis() - m_initializationTimestamp) / 1000);
    }

    /**
     * Wakes up the reorganization thread.
     */
    private void signalReorganization() {
        m_scheduler.signalReorganization(this);
    }

    /**
     * Wakes up the reorganization thread and waits until reorganization is finished.
     */
    private void signalReorganizationAndWait() {
        m_scheduler.signalReorganizationBlocking(this);
    }

}
