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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;

/**
 * This class implements the secondary log buffer.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.06.2014
 */
public final class SecondaryLogBuffer {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLogBuffer.class.getSimpleName());

    private final DirectByteBufferWrapper m_buffer;
    private final SecondaryLog m_secondaryLog;

    private final int m_logSegmentSize;

    private final ReentrantLock m_lock;

    /**
     * Creates an instance of SecondaryLogBuffer. Create a secondary log as well.
     *
     * @param p_versionBuffer
     *         the version buffer
     * @param p_owner
     *         the NodeID
     * @param p_rangeID
     *         the RangeID
     * @param p_bufferSize
     *         the secondary log buffer size
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
     *         if the secondary log could not be created.
     */
    public SecondaryLogBuffer(final Scheduler p_scheduler, final VersionBuffer p_versionBuffer, final short p_owner,
            final short p_originalOwner, final short p_rangeID, final int p_bufferSize, final long p_secondaryLogSize,
            final int p_flashPageSize, final int p_logSegmentSize, final int p_reorgUtilizationThreshold,
            final boolean p_useTimestamps, final long p_initializationTimestamp, final String p_fileName)
            throws IOException {

        m_logSegmentSize = p_logSegmentSize;
        m_lock = new ReentrantLock(false);

        m_buffer = new DirectByteBufferWrapper(p_bufferSize + 1,
                true); // One byte for segment terminator (0) which is set before writing to secLog

        m_secondaryLog =
                new SecondaryLog(p_scheduler, p_versionBuffer, p_owner, p_originalOwner, p_rangeID, p_secondaryLogSize,
                        p_flashPageSize, p_logSegmentSize, p_reorgUtilizationThreshold, p_useTimestamps,
                        p_initializationTimestamp, p_fileName);

        LOGGER.trace("Initialized secondary log buffer (%d)", p_bufferSize);

    }

    /**
     * Closes the buffer and the log.
     */
    public void close() throws IOException {
        try {
            flushSecLogBuffer();
        } catch (final IOException e) {

            LOGGER.error("Could not flush secondary log buffer", e);

        }

        m_secondaryLog.close();
    }

    /**
     * Closes the buffer and the log.
     */
    public void closeAndRemoveLog() throws IOException {
        try {
            flushSecLogBuffer();
        } catch (final IOException e) {

            LOGGER.error("Could not flush secondary log buffer", e);

        }

        m_secondaryLog.closeAndRemove();
    }

    /**
     * Returns the secondary log.
     *
     * @return the secondary log
     */
    public SecondaryLog getLog() {
        return m_secondaryLog;
    }

    /**
     * Returns the number of bytes
     *
     * @return the number of bytes
     */
    public int getOccupiedSpace() {
        return m_buffer.getBuffer().position();
    }

    /**
     * Returns whether the secondary log buffer is empty or not
     *
     * @return whether buffer is empty or not
     */
    public boolean isBufferEmpty() {
        return m_buffer.getBuffer().position() == 0;
    }

    /**
     * Flushes all data in secondary log buffer to secondary log regardless of the size
     *
     * @throws IOException
     *         if the secondary log could not be written or buffer be read
     */
    public void flushSecLogBuffer() throws IOException {
        m_lock.lock();
        if (m_buffer.getBuffer().position() > 0) {
            m_secondaryLog.postData(m_buffer, m_buffer.getBuffer().position());
            m_buffer.getBuffer().rewind();
        }
        m_lock.unlock();
    }

    /**
     * Update metadata after recovery.
     *
     * @param p_restorer
     *         NodeID of the peer which recovered the backup range
     * @param p_newRangeID
     *         the new RangeID
     * @param p_newFile
     *         the new file name
     */
    public void transferBackupRange(final short p_restorer, final short p_newRangeID, final String p_newFile)
            throws IOException {
        m_secondaryLog.transferBackupRange(p_restorer, p_newRangeID, p_newFile);
    }

    /**
     * Buffers given data in secondary log buffer or writes it in secondary log if buffer
     * contains enough data
     *
     * @param p_buffer
     *         buffer with data to append
     * @param p_entryOrRangeSize
     *         size of the log entry/range
     * @return the DirectByteBufferWrapper for flushing or null if data was appended to buffer
     */
    public DirectByteBufferWrapper bufferData(final DirectByteBufferWrapper p_buffer, final int p_entryOrRangeSize) {
        DirectByteBufferWrapper bufferWrapper;
        ByteBuffer secLogBuffer = m_buffer.getBuffer();

        m_lock.lock();
        if (secLogBuffer.position() + p_entryOrRangeSize + 1 >= secLogBuffer.capacity()) {
            // Merge current secondary log buffer and new buffer and write to secondary log
            bufferWrapper = new DirectByteBufferWrapper(secLogBuffer.position() + p_entryOrRangeSize + 1,
                    true); // One byte for segment terminator (0) which is set before writing to secLog

            secLogBuffer.flip();
            bufferWrapper.getBuffer().put(secLogBuffer);
            secLogBuffer.clear();

            processBuffer(p_buffer.getBuffer(), p_entryOrRangeSize, bufferWrapper);

            bufferWrapper.getBuffer().flip();

            // The limit might be much smaller than the capacity as log entry headers have been truncated

            m_lock.unlock();
            return bufferWrapper;
        } else {
            // Append buffer to secondary log buffer
            processBuffer(p_buffer.getBuffer(), p_entryOrRangeSize, m_buffer);

            m_lock.unlock();
            return null;
        }
    }

    /**
     * Changes log entries for storing in secondary log
     *
     * @param p_buffer
     *         the log entries
     * @param p_entryOrRangeSize
     *         size of the log entry/range
     */
    private static void processBuffer(final ByteBuffer p_buffer, final int p_entryOrRangeSize,
            final DirectByteBufferWrapper p_destination) {
        int oldBufferOffset = 0;
        int logEntrySize;
        AbstractPrimLogEntryHeader logEntryHeader;

        while (oldBufferOffset < p_entryOrRangeSize) {
            // Determine header of next log entry
            logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
            short type = (short) (p_buffer.get(oldBufferOffset) & 0xFF);
            logEntrySize =
                    logEntryHeader.getHeaderSize(type) + logEntryHeader.getLength(type, p_buffer, oldBufferOffset);

            // Copy primary log header, but skip NodeID and RangeID
            AbstractPrimLogEntryHeader.convertAndPut(p_buffer, oldBufferOffset, p_destination.getBuffer(), logEntrySize,
                    p_buffer.capacity() - oldBufferOffset, AbstractPrimLogEntryHeader.getConversionOffset(type));
            p_buffer.limit(p_buffer.capacity());
            oldBufferOffset += logEntrySize;
        }
    }

    /**
     * Flushes all data in secondary log buffer to secondary log regardless of the size
     * Appends given data to buffer data and writes all data at once
     *
     * @param p_buffer
     *         buffer with data to append
     * @param p_entryOrRangeSize
     *         size of the log entry/range
     * @throws IOException
     *         if the secondary log could not be written or buffer be read
     */
    public void flushAllDataToSecLog(final DirectByteBufferWrapper p_buffer, final int p_entryOrRangeSize)
            throws IOException {
        DirectByteBufferWrapper wrapper;
        ByteBuffer secLogBuffer;
        ByteBuffer dataToWrite;

        assert p_buffer.getBuffer().position() == 0;
        assert p_buffer.getBuffer().limit() == p_buffer.getBuffer().capacity();

        m_lock.lock();
        if (isBufferEmpty()) {
            m_lock.unlock();
            // No data in secondary log buffer -> Write directly in secondary log
            m_secondaryLog.postData(p_buffer, p_entryOrRangeSize);
        } else {
            // There is data in secondary log buffer
            secLogBuffer = m_buffer.getBuffer();
            if (secLogBuffer.position() + p_entryOrRangeSize <= p_buffer.getBuffer().capacity()) {
                // The data in secondary log buffer does fit in the buffer to flush -> prepend it
                ByteBuffer buffer = p_buffer.getBuffer();
                int secLogBufSize = secLogBuffer.position();

                buffer.limit(p_entryOrRangeSize);
                ByteBuffer slice = buffer.slice();

                buffer.limit(buffer.capacity());
                buffer.position(secLogBufSize);
                buffer.put(slice);
                buffer.position(0);

                secLogBuffer.flip();
                buffer.put(secLogBuffer);

                secLogBuffer.clear();
                buffer.position(0);

                m_lock.unlock();
                m_secondaryLog.postData(p_buffer, secLogBufSize + p_entryOrRangeSize);
            } else if (secLogBuffer.position() + p_entryOrRangeSize <= m_logSegmentSize) {
                // Data combined fits in one segment -> flush buffer and write new data in secondary log with one access
                wrapper = new DirectByteBufferWrapper(secLogBuffer.position() + p_entryOrRangeSize + 1,
                        true); // One byte for segment terminator (0) which is set before writing to secLog
                dataToWrite = wrapper.getBuffer();

                // Copy secLog buffer
                secLogBuffer.flip();
                dataToWrite.put(secLogBuffer);
                secLogBuffer.clear();

                // Copy new range
                p_buffer.getBuffer().limit(p_entryOrRangeSize);
                dataToWrite.put(p_buffer.getBuffer());

                dataToWrite.rewind();

                m_lock.unlock();
                m_secondaryLog.postData(wrapper, dataToWrite.capacity() - 1);
            } else {
                // Write buffer first
                m_secondaryLog.postData(m_buffer, secLogBuffer.position());
                secLogBuffer.rewind();
                m_lock.unlock();

                // Write new data
                m_secondaryLog.postData(p_buffer, p_entryOrRangeSize);
            }
        }
    }
}
