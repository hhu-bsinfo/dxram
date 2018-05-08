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
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.AbstractPrimLogEntryHeader;

/**
 * Primary log write buffer. Implemented as a ring buffer on a ByteBuffer. The
 * in-memory write-buffer for writing on primary log is cyclic. Similar to a
 * ring buffer all read and write accesses are done by using pointers. All
 * readable bytes are between read and write pointer. Unused bytes between write
 * and read pointer. This class is designed for one producer and one
 * consumer (primary buffer process-thread).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
public class PrimaryWriteBuffer {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrimaryWriteBuffer.class.getSimpleName());

    // Constants
    private static final int WRITE_BUFFER_MAX_SIZE = 1024 * 1024 * 1024;
    private static final int FLUSH_THRESHOLD = 16 * 1024 * 1024;
    private static final long PROCESSTHREAD_TIMEOUTTIME = 100L;

    // Attributes
    private final LogComponent m_logComponent;
    private final int m_writeBufferSize;
    private final int m_flashPageSize;
    private final int m_secondaryLogBufferSize;
    private final int m_logSegmentSize;
    private final boolean m_useChecksum;
    private final boolean m_native;

    private DirectByteBufferWrapper m_bufferWrapper;
    private ByteBuffer m_buffer;
    private PrimaryBufferProcessThread m_processThread;

    private volatile long m_bufferReadPointer;
    private volatile long m_bufferWritePointer;
    private volatile boolean m_priorityFlush;
    private HashMap<Integer, Partitioning> m_lengthAndFragmentationByBackupRange;
    private AtomicBoolean m_metadataLock;

    private BufferPool m_bufferPool;
    private WriterJobQueue m_writerJobQueue;

    private volatile boolean m_isShuttingDown;

    // Constructors

    /**
     * Creates an instance of PrimaryWriteBuffer with user-specific
     * configuration
     *
     * @param p_logComponent
     *         the log component
     * @param p_primaryLog
     *         Instance of the primary log. Used to write directly to primary log if buffer is full
     * @param p_writeBufferSize
     *         the size of the write buffer
     * @param p_flashPageSize
     *         the size of a flash page
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_useChecksum
     *         whether checksums are used
     */
    public PrimaryWriteBuffer(final LogComponent p_logComponent, final PrimaryLog p_primaryLog, final int p_writeBufferSize, final int p_flashPageSize,
            final int p_secondaryLogBufferSize, final int p_logSegmentSize, final boolean p_useChecksum) {
        m_logComponent = p_logComponent;
        m_writeBufferSize = p_writeBufferSize;
        m_flashPageSize = p_flashPageSize;
        m_secondaryLogBufferSize = p_secondaryLogBufferSize;
        m_logSegmentSize = p_logSegmentSize;
        m_useChecksum = p_useChecksum;

        m_bufferReadPointer = 0;
        m_bufferWritePointer = 0;
        m_processThread = null;
        m_priorityFlush = false;

        m_metadataLock = new AtomicBoolean(false);
        if (m_writeBufferSize < m_flashPageSize || m_writeBufferSize > WRITE_BUFFER_MAX_SIZE || Integer.bitCount(m_writeBufferSize) != 1) {
            throw new IllegalArgumentException("Illegal buffer size! Must be 2^x with " + Math.log(m_flashPageSize) / Math.log(2) + " <= x <= 31");
        }
        m_bufferWrapper = new DirectByteBufferWrapper(m_writeBufferSize, false);
        m_buffer = m_bufferWrapper.getBuffer();
        m_native = m_buffer.isDirect();
        m_lengthAndFragmentationByBackupRange = new HashMap<Integer, Partitioning>();
        m_isShuttingDown = false;

        m_bufferPool = new BufferPool(p_logSegmentSize);

        m_writerJobQueue = new WriterJobQueue(this, p_primaryLog);

        m_processThread = new PrimaryBufferProcessThread();
        m_processThread.setName("Logging: Process Thread");
        m_processThread.start();

        // #if LOGGER == TRACE
        LOGGER.trace("Initialized primary write buffer (%d)", m_writeBufferSize);
        // #endif /* LOGGER == TRACE */
    }

    // Methods

    /**
     * Cleans the write buffer and resets the pointer
     */
    public final void closeWriteBuffer() {
        // Shutdown primary buffer process and writer threads
        m_isShuttingDown = true;
        m_writerJobQueue.shutdown();
    }

    /**
     * Return ByteBuffer wrapper to pool
     *
     * @param p_bufferWrapper
     *         the buffer
     */
    void returnBuffer(final DirectByteBufferWrapper p_bufferWrapper) {
        m_bufferPool.returnBuffer(p_bufferWrapper);
    }

    /**
     * Writes log entries to primary write buffer.
     *
     * @param p_importer
     *         the message importer
     * @param p_chunkID
     *         the chunk ID
     * @param p_payloadLength
     *         the payload length
     * @param p_rangeID
     *         the range ID
     * @param p_owner
     *         the current owner
     * @param p_originalOwner
     *         the creator
     * @param p_timestamp
     *         the time since initialization in seconds
     * @param p_secLog
     *         the corresponding secondary log (to determine the version)
     */
    public final void putLogData(final AbstractMessageImporter p_importer, final long p_chunkID,
            final int p_payloadLength, final short p_rangeID, final short p_owner, final short p_originalOwner,
            final int p_timestamp, final SecondaryLog p_secLog) {
        AbstractPrimLogEntryHeader logEntryHeader;
        byte headerSize;
        int bytesToWrite;
        int bytesUntilEnd;
        int writtenBytes = 0;
        int writeSize;
        int writePointer;
        int readPointer;
        int numberOfHeaders;
        int combinedRangeID;
        ByteBuffer header;
        Version version;

        version = p_secLog.getNextVersion(p_chunkID);

        logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
        // Create log entry header and write it to a pooled buffer
        // -> easier to handle (overflow, chaining, ...) than writing directly into the primary write buffer
        // Checksum and chaining information are added in loop below
        header = logEntryHeader
                .createLogEntryHeader(p_chunkID, p_payloadLength, version, p_rangeID, p_owner, p_originalOwner,
                        p_timestamp);
        headerSize = (byte) header.limit();

        // Combine owner and range ID in an int to be used as a key in hash table
        combinedRangeID = (p_owner << 16) + p_rangeID;

        // Large chunks are split and chained -> there might be more than one header
        numberOfHeaders = (int) Math.ceil((float) p_payloadLength / AbstractLogEntryHeader.getMaxLogEntrySize());
        bytesToWrite = numberOfHeaders * headerSize + p_payloadLength;

        if (p_payloadLength <= 0) {
            throw new IllegalArgumentException("No payload for log entry!");
        }
        if (numberOfHeaders > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Chunk is too large to log. Maximum chunk size for current configuration is " +
                            Byte.MAX_VALUE * AbstractLogEntryHeader.getMaxLogEntrySize() + '!');
        }
        if (bytesToWrite > m_writeBufferSize) {
            throw new IllegalArgumentException("Data to write exceeds buffer size!");
        }

        while (true) {
            long readPointerAbsolute = m_bufferReadPointer;
            long writePointerAbsolute = m_bufferWritePointer; // We need this value for compareAndSet operation
            if ((readPointerAbsolute + m_writeBufferSize & 0x7FFFFFFF) > (writePointerAbsolute + bytesToWrite & 0x7FFFFFFF) ||
                        /* 31-bit overflow in readPointer but not posFront */
                    (readPointerAbsolute + m_writeBufferSize & 0x7FFFFFFF) < readPointerAbsolute &&
                            (writePointerAbsolute + bytesToWrite & 0x7FFFFFFF) > readPointerAbsolute) {

                // ***Appending***//
                readPointer = (int) (readPointerAbsolute % m_buffer.capacity());
                writePointer = (int) (writePointerAbsolute % m_buffer.capacity());
                for (int i = 0; i < numberOfHeaders; i++) {
                    writeSize = Math.min(bytesToWrite - writtenBytes, AbstractLogEntryHeader.getMaxLogEntrySize()) - headerSize;

                    if (numberOfHeaders > 1) {
                        // Log entry is too large and must be chained -> set chaining ID, chain size and length in header for this part
                        AbstractPrimLogEntryHeader.addChainingIDAndChainSize(header, 0, (byte) i, (byte) numberOfHeaders, logEntryHeader);
                        AbstractPrimLogEntryHeader.adjustLength(header, 0, writeSize, logEntryHeader);
                    }

                    // Determine free space from end of log to end of array
                    if (writePointer >= readPointer) {
                        bytesUntilEnd = m_writeBufferSize - writePointer;
                    } else {
                        bytesUntilEnd = readPointer - writePointer;
                    }

                    if (writeSize + headerSize <= bytesUntilEnd) {
                        // Write header
                        m_buffer.put(header);

                        // Write payload
                        if (m_native) {
                            p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(), writeSize);
                        } else {
                            p_importer.readBytes(m_buffer.array(), m_buffer.position(), writeSize);
                        }
                        m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
                    } else {
                        // Twofold cyclic write access
                        if (bytesUntilEnd < headerSize) {
                            // Write header
                            header.limit(bytesUntilEnd);
                            m_buffer.put(header);

                            header.limit(headerSize);
                            m_buffer.position(0);
                            m_buffer.put(header);

                            // Write payload
                            if (m_native) {
                                p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(), writeSize);
                            } else {
                                p_importer.readBytes(m_buffer.array(), m_buffer.position(), writeSize);
                            }
                            m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
                        } else if (bytesUntilEnd > headerSize) {
                            // Write header
                            m_buffer.put(header);

                            // Write payload
                            if (m_native) {
                                p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(),
                                        bytesUntilEnd - headerSize);

                                p_importer.readBytes(m_bufferWrapper.getAddress(), 0,
                                        writeSize - (bytesUntilEnd - headerSize));
                            } else {
                                p_importer.readBytes(m_buffer.array(), m_buffer.position(), bytesUntilEnd - headerSize);

                                p_importer.readBytes(m_buffer.array(), 0, writeSize - (bytesUntilEnd - headerSize));
                            }
                            m_buffer.position((writeSize - (bytesUntilEnd - headerSize)) % m_buffer.capacity());
                        } else {
                            // Write header
                            m_buffer.put(header);

                            // Write payload
                            if (m_native) {
                                p_importer.readBytes(m_bufferWrapper.getAddress(), 0, writeSize);
                            } else {
                                p_importer.readBytes(m_buffer.array(), 0, writeSize);
                            }
                            m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
                        }
                    }

                    if (m_useChecksum) {
                        // Determine checksum for payload and add to header
                        AbstractPrimLogEntryHeader
                                .addChecksum(m_bufferWrapper, writePointer, writeSize, logEntryHeader, headerSize,
                                        bytesUntilEnd);
                    }

                    writePointer = (writePointer + writeSize + headerSize) % m_buffer.capacity();
                    writtenBytes += writeSize + headerSize;
                }

                // Enter critical area by acquiring spin lock
                while (!m_metadataLock.compareAndSet(false, true)) {
                    // Try again
                }

                // Add bytes to write to log of combinedRangeID (optimization for sorting)
                int conversionOffset = AbstractPrimLogEntryHeader.getConversionOffset(header, 0);
                HashMap<Integer, Partitioning> hashMap = m_lengthAndFragmentationByBackupRange;
                Partitioning part = hashMap.get(combinedRangeID);
                if (part == null) {
                    hashMap.put(combinedRangeID, new Partitioning(bytesToWrite, conversionOffset));
                } else {
                    part.addEntry(bytesToWrite, conversionOffset);
                }

                // Set buffer write pointer and byte counter
                m_bufferWritePointer = writePointerAbsolute + bytesToWrite & 0x7FFFFFFF;

                // Leave critical area by resetting spin lock
                m_metadataLock.set(false);

                break;
            } else {
                // Buffer is full -> wait
                LockSupport.parkNanos(100);
            }
        }
    }

    /**
     * Wakes-up process thread and flushes data to primary log
     * Is only called by exclusive message handler
     */
    public final void initiatePriorityFlush() {
        m_priorityFlush = true;
    }

    // Classes

    /**
     * The process thread flushes data from buffer to primary log
     * after being waked-up (signal or timer)
     *
     * @author Kevin Beineke 06.06.2014
     */
    private final class PrimaryBufferProcessThread extends Thread {

        // Constructors

        /**
         * Creates an instance of PrimaryBufferProcessThread
         */
        PrimaryBufferProcessThread() {
        }

        @Override
        public void run() {
            boolean flush = false;
            long timeLastFlush = System.currentTimeMillis();
            SecondaryLogsReorgThread reorgThread = m_logComponent.getReorganizationThread();

            ByteBuffer buffer = m_buffer.duplicate();
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            while (true) {
                if (m_isShuttingDown) {
                    break;
                }

                if (m_priorityFlush) {
                    m_priorityFlush = false;
                    flush = true;
                }

                if (getBytesInBuffer() > FLUSH_THRESHOLD || System.currentTimeMillis() - timeLastFlush > PROCESSTHREAD_TIMEOUTTIME) {
                    flush = true;
                }

                if (flush) {
                    flushDataToPrimaryLog(buffer);

                    if (reorgThread == null) {
                        reorgThread = m_logComponent.getReorganizationThread();
                    } else {
                        m_logComponent.getReorganizationThread().grantAccessToCurrentLog();
                    }
                    timeLastFlush = System.currentTimeMillis();
                } else {

                    if (reorgThread == null) {
                        reorgThread = m_logComponent.getReorganizationThread();
                    } else {
                        m_logComponent.getReorganizationThread().grantAccessToCurrentLog();
                    }

                    // Wait
                    LockSupport.parkNanos(100);
                }
                flush = false;
            }
        }

        int getBytesInBuffer() {
            int readPointerUnsigned = (int) m_bufferReadPointer;
            int writePointerUnsigned = (int) m_bufferWritePointer;
            if (writePointerUnsigned >= readPointerUnsigned) {
                return writePointerUnsigned - readPointerUnsigned;
            } else {
                return (int) (writePointerUnsigned + (long) Math.pow(2, 31) - readPointerUnsigned);
            }
        }

        /**
         * Flushes all data in write buffer to primary log
         */
        void flushDataToPrimaryLog(final ByteBuffer p_primaryWriteBuffer) {
            int flushedBytes = 0;
            int bytesInWriteBuffer;
            int readPointer;
            Set<Entry<Integer, Partitioning>> lengthAndFragmentationByBackupRange;

            // Enter critical area by acquiring spin lock
            while (!m_metadataLock.compareAndSet(false, true)) {
                // Try again
            }

            bytesInWriteBuffer = getBytesInBuffer();
            readPointer = (int) m_bufferReadPointer % m_buffer.capacity();

            lengthAndFragmentationByBackupRange = m_lengthAndFragmentationByBackupRange.entrySet();
            m_lengthAndFragmentationByBackupRange = new HashMap<Integer, Partitioning>();
            // Leave critical area by resetting spin lock
            m_metadataLock.set(false);

            if (bytesInWriteBuffer > 0) {
                // Write data to secondary logs or primary log
                try {
                    p_primaryWriteBuffer.position(readPointer);
                    flushedBytes = bufferAndStore(p_primaryWriteBuffer, readPointer, bytesInWriteBuffer, lengthAndFragmentationByBackupRange);
                } catch (final IOException | InterruptedException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not flush data", e);
                    // #endif /* LOGGER >= ERROR */
                }
                if (flushedBytes != bytesInWriteBuffer) {
                    System.out.println(flushedBytes + " != " + bytesInWriteBuffer);
                    // #if LOGGER >= ERROR
                    LOGGER.error("Flush incomplete!");
                    // #endif /* LOGGER >= ERROR */
                }
            }

            m_bufferReadPointer = m_bufferReadPointer + flushedBytes & 0x7FFFFFFF;
        }

        /**
         * Writes given data to secondary log buffers or directly to secondary logs
         * if longer than a flash page. Merges consecutive log entries of the same
         * node to limit the number of write accesses
         *
         * @param p_primaryWriteBuffer
         *         data block
         * @param p_offset
         *         offset within the buffer
         * @param p_length
         *         length of data
         * @param p_lengthAndFragmentationByBackupRange
         *         length of data per node
         * @return the number of stored bytes
         * @throws IOException
         *         if secondary log (buffer) could not be written
         * @throws InterruptedException
         *         if caller is interrupted
         */
        private int bufferAndStore(final ByteBuffer p_primaryWriteBuffer, final int p_offset, final int p_length,
                final Set<Entry<Integer, Partitioning>> p_lengthAndFragmentationByBackupRange) throws InterruptedException, IOException {
            int i;
            int offset;
            int primaryLogBufferSize = 0;
            int bytesRead = 0;
            int logEntrySize;
            int bytesUntilEnd;
            int segmentLength;
            int bufferLength;
            int combinedRangeID;
            short headerSize;
            Partitioning part;
            DirectByteBufferWrapper primaryLogBuffer = null;
            ByteBuffer header;
            ByteBuffer segment;
            DirectByteBufferWrapper segmentWrapper;
            AbstractPrimLogEntryHeader logEntryHeader;
            Iterator<Entry<Integer, Partitioning>> iter;
            Entry<Integer, Partitioning> entry;
            HashMap<Integer, BufferNode> map;
            Iterator<Entry<Integer, BufferNode>> iter2;
            Entry<Integer, BufferNode> entry2;
            BufferNode bufferNode;

            // Sort buffer by backup range

            /*
             * Initialize backup range buffers:
             * For every NodeID with at least one log entry in this
             * buffer a hashmap entry will be created. The hashmap entry
             * contains the RangeID (key), a buffer fitting all log
             * entries and an offset. The size of the buffer is known
             * from the monitoring information p_lengthByBackupRange.
             * The offset is zero if the buffer will be stored in primary
             * log (complete header) The offset is two if the buffer will be
             * stored directly in secondary log (header without NodeID).
             */
            map = new HashMap<Integer, BufferNode>();
            iter = p_lengthAndFragmentationByBackupRange.iterator();
            while (iter.hasNext()) {
                entry = iter.next();
                combinedRangeID = entry.getKey();
                part = entry.getValue();
                if (part.getSecLogBytes() < m_secondaryLogBufferSize) {
                    // There is less than 128 KB (default) data from this node -> store buffer in primary log (later)
                    int totalBytes = part.getTotalBytes();
                    primaryLogBufferSize += totalBytes;
                    bufferNode = new BufferNode(totalBytes, part.m_bytesFragmentation, false);
                } else {
                    bufferNode = new BufferNode(part.getSecLogBytes(), part.m_bytesFragmentation, true);
                }
                map.put(combinedRangeID, bufferNode);
            }

            while (bytesRead < p_length) {
                offset = (p_offset + bytesRead) % p_primaryWriteBuffer.capacity();
                bytesUntilEnd = p_primaryWriteBuffer.capacity() - offset;

                logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
                /*
                 * Because of the log's wrap around three cases must be distinguished
                 * 1. Complete entry fits in current iteration
                 * 2. Offset pointer is already in next iteration
                 * 3. Log entry must be split over two iterations
                 */
                if (logEntryHeader.isReadable(p_primaryWriteBuffer, offset, bytesUntilEnd)) {
                    logEntrySize = logEntryHeader.getHeaderSize(p_primaryWriteBuffer, offset) + logEntryHeader.getLength(p_primaryWriteBuffer, offset);
                    combinedRangeID = (logEntryHeader.getOwner(p_primaryWriteBuffer, offset) << 16) + logEntryHeader.getRangeID(p_primaryWriteBuffer, offset);

                    bufferNode = map.get(combinedRangeID);
                    bufferNode.appendToBuffer(p_primaryWriteBuffer, offset, logEntrySize, bytesUntilEnd,
                            AbstractPrimLogEntryHeader.getConversionOffset(p_primaryWriteBuffer, offset));
                } else {
                    // Buffer overflow -> header is split
                    // To get header size only the first byte is necessary
                    headerSize = logEntryHeader.getHeaderSize(p_primaryWriteBuffer, offset);
                    if (m_native) {
                        header = ByteBuffer.allocateDirect(headerSize);
                    } else {
                        header = ByteBuffer.allocate(headerSize);
                    }
                    header.order(ByteOrder.LITTLE_ENDIAN);

                    header.put(p_primaryWriteBuffer);

                    p_primaryWriteBuffer.position(0);
                    p_primaryWriteBuffer.limit(headerSize - bytesUntilEnd);
                    header.put(p_primaryWriteBuffer);
                    p_primaryWriteBuffer.limit(p_primaryWriteBuffer.capacity());

                    logEntrySize = headerSize + logEntryHeader.getLength(header, 0);
                    combinedRangeID = (logEntryHeader.getOwner(header, 0) << 16) + logEntryHeader.getRangeID(header, 0);

                    bufferNode = map.get(combinedRangeID);
                    bufferNode.appendToBuffer(p_primaryWriteBuffer, offset, logEntrySize, bytesUntilEnd,
                            AbstractPrimLogEntryHeader.getConversionOffset(header, 0));
                }
                bytesRead += logEntrySize;
            }

            // Write sorted buffers to log
            if (primaryLogBufferSize > 0) {
                primaryLogBuffer = new DirectByteBufferWrapper(primaryLogBufferSize, true);
            }

            iter2 = map.entrySet().iterator();
            while (iter2.hasNext()) {
                i = 0;
                entry2 = iter2.next();
                combinedRangeID = entry2.getKey();
                bufferNode = entry2.getValue();
                bufferLength = bufferNode.getBufferLength();

                segmentWrapper = bufferNode.getSegmentWrapper(i);
                while (segmentWrapper != null) {
                    segment = segmentWrapper.getBuffer();
                    segmentLength = bufferNode.getSegmentLength(i);
                    segment.rewind();

                    if (segmentLength == 0) {
                        break;
                    }

                    if (bufferLength < m_secondaryLogBufferSize) {
                        // 1. Buffer in secondary log buffer
                        DirectByteBufferWrapper combinedBuffer =
                                bufferLogEntryInSecondaryLogBuffer(segmentWrapper, segmentLength, (short) combinedRangeID, (short) (combinedRangeID >> 16));
                        if (combinedBuffer != null) {
                            // Flush combined buffer (old data in secondary log buffer + new data)
                            writeToSecondaryLog(combinedBuffer, combinedBuffer.getBuffer().limit(), (short) combinedRangeID, (short) (combinedRangeID >> 16));
                        } else {
                            // 2. Copy log entry/range to write it in primary log subsequently if the buffer was not flushed during appending
                            assert primaryLogBuffer != null;

                            segment.position(0);
                            segment.limit(segmentLength);
                            primaryLogBuffer.getBuffer().put(segment);
                        }
                        returnBuffer(segmentWrapper);
                    } else {
                        // Segment is larger than secondary log buffer size -> skip primary log
                        writeToSecondaryLog(segmentWrapper, segmentLength, (short) combinedRangeID, (short) (combinedRangeID >> 16));
                    }
                    segmentWrapper = bufferNode.getSegmentWrapper(++i);
                }
                iter2.remove();
            }

            if (primaryLogBufferSize > 0) {
                // Write all log entries, that were not written to secondary log, in primary log with one write access
                writeToPrimaryLog(primaryLogBuffer);
            }

            return bytesRead;
        }

        /**
         * Buffers an log entry or log entry range in corresponding secondary log
         * buffer
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
        private DirectByteBufferWrapper bufferLogEntryInSecondaryLogBuffer(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize,
                final short p_rangeID, final short p_owner) throws IOException, InterruptedException {
            return m_logComponent.getSecondaryLogBuffer(p_owner, p_rangeID).bufferData(p_buffer, p_logEntrySize);
        }

        /**
         * Writes a log entry/range directly to secondary log buffer if longer than
         * secondary log buffer size Has to flush the corresponding secondary log buffer if not
         * empty to maintain order
         *
         * @param p_buffer
         *         data block
         * @param p_logEntrySize
         *         size of log entry/range
         * @param p_rangeID
         *         the RangeID
         * @param p_owner
         *         the owner NodeID
         * @throws IOException
         *         if secondary log could not be written
         * @throws InterruptedException
         *         if caller is interrupted
         */
        private void writeToSecondaryLog(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize, final short p_rangeID, final short p_owner)
                throws IOException, InterruptedException {
            m_writerJobQueue.pushJob((byte) 0, m_logComponent.getSecondaryLogBuffer(p_owner, p_rangeID), p_buffer, p_logEntrySize);
        }

        /**
         * Writes a log entry/range to primary log
         *
         * @param p_buffer
         *         data block
         */
        private void writeToPrimaryLog(final DirectByteBufferWrapper p_buffer) {
            m_writerJobQueue.pushJob((byte) 1, null, p_buffer, 0);
        }
    }

    /**
     * BufferNode
     *
     * @author Kevin Beineke 11.08.2014
     */
    private final class BufferNode {

        // Attributes
        private int m_numberOfSegments;
        private int m_currentSegment;
        private int m_bufferLength;
        private boolean m_convert;
        private DirectByteBufferWrapper[] m_segments;

        // Constructors

        /**
         * Creates an instance of BufferNode
         *
         * @param p_length
         *         the buffer length (the length might change after converting the headers and fitting the data into
         *         segments)
         * @param p_convert
         *         whether the log entry headers have to be converted or not
         */
        private BufferNode(final int p_length, final int p_fragmentation, final boolean p_convert) {
            int length = p_length + p_fragmentation;

            m_numberOfSegments = (int) Math.ceil((double) length / m_logSegmentSize);

            m_currentSegment = 0;
            m_bufferLength = p_length;
            m_convert = p_convert;

            m_segments = new DirectByteBufferWrapper[m_numberOfSegments];

            for (int i = 0; length > 0; i++) {
                m_segments[i] = m_bufferPool.getBuffer(length);
                length -= m_segments[i].getBuffer().capacity();
            }
        }

        // Getter

        /**
         * Returns the size of the unprocessed data
         *
         * @return the size of the unprocessed data
         */
        private int getBufferLength() {
            return m_bufferLength;
        }

        /**
         * Returns the number of written bytes per segment
         *
         * @param p_index
         *         the index
         * @return the number of written bytes per segment
         */
        private int getSegmentLength(final int p_index) {
            int ret = 0;

            if (p_index < m_numberOfSegments) {
                ret = m_segments[p_index].getBuffer().position();
            }

            return ret;
        }

        // Setter

        /**
         * Returns the buffer
         *
         * @param p_index
         *         the index
         * @return the buffer
         */
        private DirectByteBufferWrapper getSegmentWrapper(final int p_index) {
            DirectByteBufferWrapper ret = null;

            if (p_index < m_numberOfSegments) {
                ret = m_segments[p_index];
            }

            return ret;
        }

        // Methods

        /**
         * Appends data to node buffer
         *
         * @param p_primaryWriteBuffer
         *         the buffer
         * @param p_offset
         *         the offset within the buffer
         * @param p_logEntrySize
         *         the log entry size
         * @param p_bytesUntilEnd
         *         the number of bytes until end
         * @param p_conversionOffset
         *         the conversion offset
         */
        private void appendToBuffer(final ByteBuffer p_primaryWriteBuffer, final int p_offset, final int p_logEntrySize,
                final int p_bytesUntilEnd, final short p_conversionOffset) {
            int logEntrySize;
            ByteBuffer segment;

            if (m_convert) {
                logEntrySize = p_logEntrySize - (p_conversionOffset - 1);
            } else {
                logEntrySize = p_logEntrySize;
            }

            segment = m_segments[m_currentSegment].getBuffer();
            if (logEntrySize > segment.remaining()) {
                segment = m_segments[++m_currentSegment].getBuffer();
            }

            if (m_convert) {
                // More secondary log buffer size for this node: Convert primary log entry header to secondary log header and append entry to node buffer
                AbstractPrimLogEntryHeader.convertAndPut(p_primaryWriteBuffer, p_offset, segment, p_logEntrySize, p_bytesUntilEnd, p_conversionOffset);
            } else {
                // Less secondary log buffer size for this node: Just append entry to node buffer without converting the log entry header
                if (p_logEntrySize <= p_bytesUntilEnd) {
                    p_primaryWriteBuffer.position(p_offset);
                    p_primaryWriteBuffer.limit(p_offset + p_logEntrySize);
                    segment.put(p_primaryWriteBuffer);
                } else {
                    p_primaryWriteBuffer.position(p_offset);
                    segment.put(p_primaryWriteBuffer);

                    p_primaryWriteBuffer.position(0);
                    p_primaryWriteBuffer.limit(p_logEntrySize - p_bytesUntilEnd);
                    segment.put(p_primaryWriteBuffer);
                }
            }
            p_primaryWriteBuffer.limit(p_primaryWriteBuffer.capacity());

        }
    }

    public class Partitioning {

        private int m_totalBytes;
        private int m_bytesWithTruncatedHeaders;
        private int m_bytesFragmentation;

        Partitioning(final int p_initialBytes, final int p_conversionOffset) {
            m_totalBytes = p_initialBytes;
            m_bytesWithTruncatedHeaders = p_initialBytes - p_conversionOffset + 1;
            m_bytesFragmentation = 0;
        }

        int getTotalBytes() {
            return m_totalBytes;
        }

        int getSecLogBytes() {
            return m_bytesWithTruncatedHeaders;
        }

        int getFragmentedBytes() {
            return m_bytesFragmentation;
        }

        void addEntry(final int p_length, final int p_conversionOffset) {
            if ((m_bytesWithTruncatedHeaders + p_length) % m_logSegmentSize < p_length) {
                m_bytesFragmentation += m_logSegmentSize - m_bytesWithTruncatedHeaders % m_logSegmentSize;
            }

            m_totalBytes += p_length;
            m_bytesWithTruncatedHeaders += p_length - p_conversionOffset + 1;
        }
    }
}
