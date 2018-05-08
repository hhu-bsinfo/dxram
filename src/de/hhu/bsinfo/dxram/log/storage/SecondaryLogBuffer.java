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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.header.AbstractPrimLogEntryHeader;

/**
 * This class implements the secondary log buffer
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.06.2014
 */
public final class SecondaryLogBuffer {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLogBuffer.class.getSimpleName());

    // Attributes
    private DirectByteBufferWrapper m_buffer;
    private SecondaryLog m_secondaryLog;

    private int m_logSegmentSize;

    private ReentrantLock m_lock;

    // Getter

    /**
     * Creates an instance of SecondaryLogBuffer
     *
     * @param p_secondaryLog
     *         Instance of the corresponding secondary log. Used to write directly to secondary
     * @param p_bufferSize
     *         the secondary log buffer size
     * @param p_logSegmentSize
     *         the segment size
     */
    SecondaryLogBuffer(final SecondaryLog p_secondaryLog, final int p_bufferSize, final int p_logSegmentSize) {

        m_secondaryLog = p_secondaryLog;
        m_logSegmentSize = p_logSegmentSize;
        m_lock = new ReentrantLock(false);

        m_buffer = new DirectByteBufferWrapper(p_bufferSize + 1,
                true); // One byte for segment terminator (0) which is set before writing to secLog
        // #if LOGGER == TRACE
        LOGGER.trace("Initialized secondary log buffer (%d)", p_bufferSize);
        // #endif /* LOGGER == TRACE */
    }

    // Methods

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
     * Closes the buffer
     */
    public void close() {
        try {
            flushSecLogBuffer();
        } catch (final IOException | InterruptedException e) {

            // #if LOGGER >= ERROR
            LOGGER.error("Could not flush secondary log buffer", e);
            // #endif /* LOGGER >= ERROR */
        }
        m_buffer = null;
    }

    /**
     * Flushes all data in secondary log buffer to secondary log regardless of the size
     *
     * @throws IOException
     *         if the secondary log could not be written or buffer be read
     * @throws InterruptedException
     *         if the caller was interrupted
     */
    public void flushSecLogBuffer() throws IOException, InterruptedException {
        m_lock.lock();
        if (m_buffer.getBuffer().position() > 0) {
            m_secondaryLog.appendData(m_buffer, m_buffer.getBuffer().position());
            m_buffer.getBuffer().rewind();
        }
        m_lock.unlock();
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
    DirectByteBufferWrapper bufferData(final DirectByteBufferWrapper p_buffer, final int p_entryOrRangeSize) {
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
            logEntrySize = logEntryHeader.getHeaderSize(p_buffer, oldBufferOffset) +
                    logEntryHeader.getLength(p_buffer, oldBufferOffset);

            // Copy primary log header, but skip NodeID and RangeID
            AbstractPrimLogEntryHeader.convertAndPut(p_buffer, oldBufferOffset, p_destination.getBuffer(), logEntrySize,
                    p_buffer.capacity() - oldBufferOffset,
                    AbstractPrimLogEntryHeader.getConversionOffset(p_buffer, oldBufferOffset));
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
     * @throws InterruptedException
     *         if the caller was interrupted
     */
    void flushAllDataToSecLog(final DirectByteBufferWrapper p_buffer, final int p_entryOrRangeSize)
            throws IOException, InterruptedException {
        DirectByteBufferWrapper wrapper;
        ByteBuffer secLogBuffer;
        ByteBuffer dataToWrite;

        assert p_buffer.getBuffer().position() == 0;
        assert p_buffer.getBuffer().limit() == p_buffer.getBuffer().capacity();

        m_lock.lock();
        if (isBufferEmpty()) {
            m_lock.unlock();
            // No data in secondary log buffer -> Write directly in secondary log
            m_secondaryLog.appendData(p_buffer, p_entryOrRangeSize);
        } else {
            // There is data in secondary log buffer
            secLogBuffer = m_buffer.getBuffer();
            if (secLogBuffer.position() + p_entryOrRangeSize <= p_buffer.getBuffer().capacity()) {
                // The data in secondary log buffer does fit in the buffer to flush -> prepend it
                ByteBuffer buffer = p_buffer.getBuffer();
                int secLogBufSize = secLogBuffer.position();

                buffer.limit(p_entryOrRangeSize);
                ByteBuffer slice = buffer.slice();
                buffer.position(secLogBufSize);
                buffer.limit(buffer.capacity());
                buffer.put(slice);
                buffer.position(0);

                secLogBuffer.flip();
                buffer.put(secLogBuffer);

                secLogBuffer.clear();
                buffer.position(0);

                m_lock.unlock();
                m_secondaryLog.appendData(p_buffer, secLogBufSize + p_entryOrRangeSize);
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
                m_secondaryLog.appendData(wrapper, dataToWrite.capacity() - 1);
            } else {
                // Write buffer first
                m_secondaryLog.appendData(m_buffer, secLogBuffer.position());
                secLogBuffer.rewind();
                m_lock.unlock();

                // Write new data
                m_secondaryLog.appendData(p_buffer, p_entryOrRangeSize);
            }
        }
    }
}
