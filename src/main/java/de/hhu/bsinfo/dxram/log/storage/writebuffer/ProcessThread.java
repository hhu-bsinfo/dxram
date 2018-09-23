/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating
 * Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.logs.LogHandler;
import de.hhu.bsinfo.dxutils.hashtable.GenericHashTable;
import de.hhu.bsinfo.dxutils.hashtable.HashTableElement;

/**
 * The process thread flushes data from buffer to primary log or secondary log after being waked-up (signal or timer).
 * It does not write to disk but registers write jobs for the writer thread.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
final class ProcessThread extends Thread {

    private static final long PROCESSTHREAD_TIMEOUTTIME = 100L;

    private static final Logger LOGGER = LogManager.getFormatterLogger(ProcessThread.class.getSimpleName());

    private final Scheduler m_scheduler;
    private final LogHandler m_logHandler;

    private final WriteBuffer m_writeBuffer;
    private final BufferPool m_bufferPool;
    private final GenericHashTable<BufferNode> m_rangeBufferHashTable;

    private final int m_flushThreshold;
    private final int m_secondaryLogBufferSize;

    private volatile boolean m_isShuttingDown;

    /**
     * Creates an instance of ProcessThread.
     *
     * @param p_logHandler
     *         the log handler needed for posting write jobs
     * @param p_scheduler
     *         the scheduler to grant access to the reorganization thread
     * @param p_writeBuffer
     *         the write buffer for acquiring buffer segment to flush
     * @param p_bufferPool
     *         the buffer pool
     * @param p_flushThreshold
     *         the flush threshold
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     */
    ProcessThread(final LogHandler p_logHandler, final Scheduler p_scheduler, final WriteBuffer p_writeBuffer,
            final BufferPool p_bufferPool, final int p_flushThreshold, final int p_secondaryLogBufferSize) {
        m_logHandler = p_logHandler;
        m_scheduler = p_scheduler;

        m_writeBuffer = p_writeBuffer;
        m_flushThreshold = p_flushThreshold;
        m_secondaryLogBufferSize = p_secondaryLogBufferSize;
        m_bufferPool = p_bufferPool;

        m_rangeBufferHashTable = new GenericHashTable<>();
    }

    @Override
    public void run() {
        boolean flush = false;
        long timeLastFlush = System.currentTimeMillis();

        while (!m_isShuttingDown) {
            if (m_writeBuffer.needsToBeFlushed()) {
                flush = true;
            }

            if (m_writeBuffer.getBytesInBuffer() > m_flushThreshold ||
                    System.currentTimeMillis() - timeLastFlush > PROCESSTHREAD_TIMEOUTTIME) {
                flush = true;
            }

            if (flush) {
                flush();

                m_scheduler.grantAccessToReorganization();
                timeLastFlush = System.currentTimeMillis();
            } else {
                m_scheduler.grantAccessToReorganization();

                // Wait
                LockSupport.parkNanos(100);
            }
            flush = false;
        }
    }

    /**
     * Shuts down this thread.
     */
    void close() {
        m_isShuttingDown = true;

        try {
            join();
        } catch (InterruptedException e) {
            LOGGER.warn(e);
        }
    }

    /**
     * Flushes the write buffer.
     */
    private void flush() {
        AtomicMetadata metadata = m_writeBuffer.getMetadata();

        if (metadata != null) {
            sortAndPost(metadata.getByteBuffer(), metadata.getTotalLength(), metadata.getAllLengths());

            if (metadata.getTotalLength() > 0) {
                m_writeBuffer.updateMetadata(metadata.getTotalLength());
            }
        }
    }

    /**
     * Writes given data to primary log and secondary log buffers or directly to secondary logs
     * if longer than x flash pages.
     * Merges consecutive log entries of the same node to limit the number of write accesses.
     *
     * @param p_writeBuffer
     *         data block
     * @param p_length
     *         length of data
     * @param p_lengthByBackupRange
     *         length of data per node
     */
    private void sortAndPost(final ByteBuffer p_writeBuffer, final int p_length,
            final List<int[]> p_lengthByBackupRange) {

        int primaryLogBufferSize = distributeLogEntriesToBuffers(p_writeBuffer, p_length, p_lengthByBackupRange);

        postBuffers(primaryLogBufferSize);
    }

    /**
     * Sorts log entries by backup range by writing log entries to byte buffers (wrapped in buffer nodes).
     *
     * @param p_writeBuffer
     *         the write buffer as ByteBuffer
     * @param p_length
     *         the total number of bytes to flush
     * @param p_lengthByBackupRange
     *         the number of bytes per backup range
     * @return the number of bytes to be written to primary log
     */
    private int distributeLogEntriesToBuffers(final ByteBuffer p_writeBuffer, final int p_length,
            final List<int[]> p_lengthByBackupRange) {
        int i;
        int initialOffset = p_writeBuffer.position();
        int offset;
        int primaryLogBufferSize = 0;
        int bytesRead = 0;
        int logEntrySize;
        int bytesUntilEnd;
        int combinedRangeID;
        int rangeLength;
        short headerSize;
        ByteBuffer header;
        AbstractPrimLogEntryHeader logEntryHeader;
        BufferNode bufferNode;

        m_rangeBufferHashTable.clear();
        for (i = 0; i < p_lengthByBackupRange.size(); i++) {
            int[] array = p_lengthByBackupRange.get(i);
            combinedRangeID = array[0];
            rangeLength = array[1];

            if (rangeLength < m_secondaryLogBufferSize) {
                // There is less than 128 KB (default) data from this node -> store buffer in primary log (later)
                primaryLogBufferSize += rangeLength;
                bufferNode = new BufferNode(rangeLength, false, m_secondaryLogBufferSize, m_bufferPool);
            } else {
                bufferNode = new BufferNode(rangeLength, true, m_secondaryLogBufferSize, m_bufferPool);
            }
            m_rangeBufferHashTable.put(combinedRangeID, bufferNode);
        }

        while (bytesRead < p_length) {
            offset = (initialOffset + bytesRead) % p_writeBuffer.capacity();
            bytesUntilEnd = p_writeBuffer.capacity() - offset;

            short type = (short) (p_writeBuffer.get(offset) & 0xFF);

            logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
            /*
             * Because of the log's wrap around three cases must be distinguished
             * 1. Complete entry fits in current iteration
             * 2. Offset pointer is already in next iteration
             * 3. Log entry must be split over two iterations
             */
            if (logEntryHeader.isReadable(type, bytesUntilEnd)) {
                logEntrySize =
                        logEntryHeader.getHeaderSize(type) + logEntryHeader.getLength(type, p_writeBuffer, offset);
                combinedRangeID = (logEntryHeader.getOwner(p_writeBuffer, offset) << 16) +
                        logEntryHeader.getRangeID(p_writeBuffer, offset);

                bufferNode = m_rangeBufferHashTable.get(combinedRangeID);
                bufferNode.appendToBuffer(p_writeBuffer, offset, logEntrySize, bytesUntilEnd,
                        AbstractPrimLogEntryHeader.getConversionOffset(type), m_bufferPool);
            } else {
                // Buffer overflow -> header is split
                headerSize = logEntryHeader.getHeaderSize(type);
                if (p_writeBuffer.isDirect()) {
                    header = ByteBuffer.allocateDirect(headerSize);
                } else {
                    header = ByteBuffer.allocate(headerSize);
                }
                header.order(ByteOrder.LITTLE_ENDIAN);

                header.put(p_writeBuffer);

                p_writeBuffer.position(0);
                p_writeBuffer.limit(headerSize - bytesUntilEnd);
                header.put(p_writeBuffer);
                p_writeBuffer.limit(p_writeBuffer.capacity());

                type = (short) (header.get(0) & 0xFF);
                logEntrySize = headerSize + logEntryHeader.getLength(type, header, 0);
                combinedRangeID = (logEntryHeader.getOwner(header, 0) << 16) + logEntryHeader.getRangeID(header, 0);

                bufferNode = m_rangeBufferHashTable.get(combinedRangeID);
                bufferNode.appendToBuffer(p_writeBuffer, offset, logEntrySize, bytesUntilEnd,
                        AbstractPrimLogEntryHeader.getConversionOffset(type), m_bufferPool);
            }
            bytesRead += logEntrySize;
        }

        return primaryLogBufferSize;
    }

    /**
     * Posts buffers for the writer thread to be written either to primary log or the corresponding secondary log of
     * the backup range.
     * Note: we cannot return the number of posted bytes here to move the read pointer in write buffer
     * as processed data might differ in size (because of trimmed headers and fragmentation).
     * Therefore all data must be posted, we cannot return earlier.
     *
     * @param p_primaryLogBufferSize
     *         the number of bytes to be written to primary log
     */
    private void postBuffers(final int p_primaryLogBufferSize) {
        int i;
        int segmentLength;
        int combinedRangeID;
        boolean readyForSecLog;
        DirectByteBufferWrapper primaryLogBuffer = null;
        ByteBuffer segment;
        DirectByteBufferWrapper segmentWrapper;
        BufferNode bufferNode;

        if (p_primaryLogBufferSize > 0 && LogComponent.TWO_LEVEL_LOGGING_ACTIVATED) {
            primaryLogBuffer = new DirectByteBufferWrapper(p_primaryLogBufferSize + 1, true);
        }

        List<HashTableElement<BufferNode>> list = m_rangeBufferHashTable.convert();
        for (int j = 0; j < list.size(); j++) {
            i = 0;
            HashTableElement<BufferNode> element = list.get(j);
            combinedRangeID = element.getKey();
            bufferNode = element.getValue();
            readyForSecLog = bufferNode.readyForSecLog();

            segmentWrapper = bufferNode.getSegmentWrapper(i);
            while (segmentWrapper != null) {
                segment = segmentWrapper.getBuffer();
                segmentLength = bufferNode.getSegmentLength(i);
                segment.rewind();

                if (segmentLength == 0) {
                    // We did not need this segment as log entry headers for secondary logs are smaller
                    m_bufferPool.returnBuffer(segmentWrapper);
                    break;
                }

                if (readyForSecLog) {
                    // Segment is larger than secondary log buffer size -> skip primary log
                    writeToSecondaryLog(segmentWrapper, segmentLength, combinedRangeID, true);
                } else {
                    // 1. Buffer in secondary log buffer
                    DirectByteBufferWrapper combinedBuffer =
                            bufferLogEntryInSecondaryLogBuffer(segmentWrapper, segmentLength, (short) combinedRangeID,
                                    (short) (combinedRangeID >> 16));
                    if (combinedBuffer != null) {
                        // Flush combined buffer (old data in secondary log buffer + new data)
                        int length = combinedBuffer.getBuffer().limit();
                        combinedBuffer.getBuffer().limit(combinedBuffer.getBuffer().capacity());
                        writeToSecondaryLog(combinedBuffer, length, combinedRangeID, false);
                    } else if (LogComponent.TWO_LEVEL_LOGGING_ACTIVATED) {
                        // 2. Copy log entry/range to write it to primary log subsequently if the buffer
                        // was not flushed during appending
                        assert primaryLogBuffer != null;

                        segment.position(0);
                        segment.limit(segmentLength);
                        primaryLogBuffer.getBuffer().put(segment);
                    }
                    m_bufferPool.returnBuffer(segmentWrapper);
                }
                segmentWrapper = bufferNode.getSegmentWrapper(++i);
            }
        }

        if (p_primaryLogBufferSize > 0 && LogComponent.TWO_LEVEL_LOGGING_ACTIVATED) {
            // Write all log entries, that were not written to secondary log, in primary log with one write access
            int length = primaryLogBuffer.getBuffer().position();
            primaryLogBuffer.getBuffer().rewind();
            writeToPrimaryLog(primaryLogBuffer, length);
        }
    }

    /**
     * Buffers an log entry or log entry range in corresponding secondary log buffer.
     *
     * @param p_buffer
     *         data block
     * @param p_logEntrySize
     *         size of log entry/range
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the owner NodeID
     * @return the DirectByteBufferWrapper for flushing or null if data was appended to buffer
     */
    private DirectByteBufferWrapper bufferLogEntryInSecondaryLogBuffer(final DirectByteBufferWrapper p_buffer,
            final int p_logEntrySize, final short p_rangeID, final short p_owner) {

        assert p_buffer.getBuffer().limit() == p_buffer.getBuffer().capacity();

        return m_logHandler.bufferDataForSecondaryLog(p_buffer, p_logEntrySize, p_rangeID, p_owner);
    }

    /**
     * Writes a log entry/range directly to secondary log buffer if longer than secondary log buffer size.
     * Has to flush the corresponding secondary log buffer if not empty to maintain order.
     *
     * @param p_buffer
     *         data block
     * @param p_logEntrySize
     *         size of log entry/range
     * @param p_combinedRangeID
     *         the RangeID and owner NodeID
     * @param p_returnBuffer
     *         whether to return the write buffer to buffer pool or not
     */
    private void writeToSecondaryLog(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize,
            final int p_combinedRangeID, final boolean p_returnBuffer) {

        assert p_buffer.getBuffer().limit() == p_buffer.getBuffer().capacity();

        m_logHandler.writeToSecondaryLog(p_buffer, p_logEntrySize, p_combinedRangeID, p_returnBuffer);
    }

    /**
     * Writes all gathered log entries/ranges to primary log.
     *
     * @param p_buffer
     *         data block
     * @param p_logEntrySize
     *         size of log entry/range
     */
    private void writeToPrimaryLog(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize) {

        assert p_buffer.getBuffer().limit() == p_buffer.getBuffer().capacity();

        m_logHandler.writeToPrimaryLog(p_buffer, p_logEntrySize);
    }
}
