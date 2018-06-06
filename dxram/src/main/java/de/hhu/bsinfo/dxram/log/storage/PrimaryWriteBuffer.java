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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.AbstractPrimLogEntryHeader;

/**
 * Primary log write buffer Implemented as a ring buffer in a byte array. The
 * in-memory write-buffer for writing on primary log is cyclic. Similar to a
 * ring buffer all read and write accesses are done by using pointers. All
 * readable bytes are between read and write pointer. Unused bytes between write
 * and read pointer. This class is designed for several producers and one
 * consumer (primary log writer- thread).Therefore the write-buffer is
 * implemented thread-safely. There are two write modes. In default mode the
 * buffer is extended adaptively if a threshold is passed (in (flash page size)
 * steps or doubled). Alternatively the caller can be blocked until the write
 * access is completed.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
public class PrimaryWriteBuffer {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrimaryWriteBuffer.class.getSimpleName());

    // Constants
    private static final int WRITE_BUFFER_MAX_SIZE = 1024 * 1024 * 1024;
    // Must be smaller than 1/2 of WRITE_BUFFER_SIZE
    private static final int SIGNAL_ON_BYTE_COUNT = 16 * 1024 * 1024;
    private static final long WRITERTHREAD_TIMEOUTTIME = 100L;

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
    private PrimaryLogProcessThread m_processThread;

    private long m_timestamp;

    // The following members are read/set by writer thread synchronized by metadata lock and read/set by exclusive message handler without
    // synchronization, but it is guaranteed that metadata lock is acquired by exclusive message handler before writer thread accesses
    // the metadata (-> happened before)
    private int m_bufferReadPointer;
    private int m_bufferWritePointer;
    private HashMap<Integer, Partitioning> m_lengthAndFragmentationByBackupRange;

    // Read/Set by exclusive message handler, only (either to log chunks or to initialize flushing during recovery)
    private boolean m_needToLock;

    // All accesses are synchronized by metadata lock
    private boolean m_dataAvailable;

    // Read by writer thread, set by application thread
    private volatile boolean m_isShuttingDown;

    // Set by application thread(s); used to wait until flushing is completed
    private volatile boolean m_flushingComplete;

    // Written by writer thread, read by exclusive message handler
    private volatile boolean m_writerThreadRequestsAccessToBuffer;

    // Read/Set by exclusive message handler and writer thread; used to grant access to writer thread
    private volatile boolean m_writerThreadAccessesBuffer;

    // Read and increased by exclusive message handler, read and reset by writer thread (no concurrent access!)
    private volatile int m_bytesInWriteBuffer;

    private ReentrantLock m_metadataLock;
    private Condition m_dataAvailableCond;
    private Condition m_finishedCopyingCond;

    private BufferPool m_bufferPool;
    private WriterJobQueue m_writerJobQueue;

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
        m_bytesInWriteBuffer = 0;
        m_timestamp = System.currentTimeMillis();
        m_processThread = null;
        m_flushingComplete = false;
        m_dataAvailable = false;

        m_needToLock = false;

        m_metadataLock = new ReentrantLock(false);
        m_dataAvailableCond = m_metadataLock.newCondition();
        m_finishedCopyingCond = m_metadataLock.newCondition();

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

        m_processThread = new PrimaryLogProcessThread();
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
        // Shutdown primary log writer-thread
        m_flushingComplete = false;
        m_isShuttingDown = true;
        while (!m_flushingComplete) {
            Thread.yield();
        }
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
     * @throws InterruptedException
     *         if caller is interrupted
     */
    public final void putLogData(final AbstractMessageImporter p_importer, final long p_chunkID, final int p_payloadLength, final short p_rangeID,
            final short p_owner, final short p_originalOwner, final int p_timestamp, final SecondaryLog p_secLog) throws InterruptedException {
        AbstractPrimLogEntryHeader logEntryHeader;
        byte headerSize;
        int bytesInWriteBuffer;
        int bytesToWrite;
        int bytesUntilEnd;
        int writtenBytes = 0;
        int writeSize;
        int writePointer;
        int numberOfHeaders;
        int combinedRangeID;
        long currentTime;
        ByteBuffer header;
        Version version;

        version = p_secLog.getNextVersion(p_chunkID);

        logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
        header = logEntryHeader.createLogEntryHeader(p_chunkID, p_payloadLength, version, p_rangeID, p_owner, p_originalOwner, p_timestamp);
        headerSize = (byte) header.limit();

        combinedRangeID = (p_owner << 16) + p_rangeID;

        // Large chunks are split and chained -> there might be more than one header
        numberOfHeaders = (int) Math.ceil((float) p_payloadLength / AbstractLogEntryHeader.getMaxLogEntrySize());
        bytesToWrite = numberOfHeaders * headerSize + p_payloadLength;

        if (p_payloadLength <= 0) {
            throw new IllegalArgumentException("No payload for log entry!");
        }
        if (numberOfHeaders > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Chunk is too large to log. Maximum chunk size for current configuration is " +
                    Byte.MAX_VALUE * AbstractLogEntryHeader.getMaxLogEntrySize() + '!');
        }
        if (bytesToWrite > m_writeBufferSize) {
            throw new IllegalArgumentException("Data to write exceeds buffer size!");
        }

        // ***Synchronization***//
        // Signal writer thread if write buffer is nearly full
        bytesInWriteBuffer = m_bytesInWriteBuffer;
        if (bytesInWriteBuffer > m_writeBufferSize * 0.8) {
            // Grant writer thread access to meta-data (data available)
            // Acquires metadata lock -> changed values become visible to writer thread
            grantAccessToWriterThread();
        }

        // Explicitly synchronize if access to meta-data was granted to writer thread or no message was logged in a long time
        // This is always executed by the same thread (exclusive message handler)
        currentTime = System.currentTimeMillis();
        if (m_needToLock || currentTime - m_timestamp > WRITERTHREAD_TIMEOUTTIME) {
            // If m_needToLock is set, writer thread got access granted by this thread and flushing is in progress
            // After waiting at least WRITERTHREAD_TIMEOUTTIME without getting access, the writer thread accesses the metadata
            // anyway -> check if writer thread is accessing when (currentTime - m_timestamp > WRITERTHREAD_TIMEOUTTIME)
            m_needToLock = false;
            m_metadataLock.lock();
            while (m_writerThreadAccessesBuffer) {
                m_finishedCopyingCond.await();
            }
            m_metadataLock.unlock();
            bytesInWriteBuffer = 0;
        }
        m_timestamp = currentTime;

        // ***Appending***//
        // Set buffer write pointer and byte counter
        // Access metadata without locking as no other thread can change meanwhile; changed values will become visible
        // when granted access to writer thread by acquiring the metadata lock (-> happened before)
        writePointer = m_bufferWritePointer;

        m_bufferWritePointer = (writePointer + bytesToWrite) % m_buffer.capacity();
        // Update byte counters
        m_bytesInWriteBuffer += bytesToWrite;

        // Add bytes to write to log of combinedRangeID (optimization for sorting)
        int conversionOffset = AbstractPrimLogEntryHeader.getConversionOffset(header, 0);
        Partitioning part = m_lengthAndFragmentationByBackupRange.get(combinedRangeID);
        if (part == null) {
            m_lengthAndFragmentationByBackupRange.put(combinedRangeID, new Partitioning(bytesToWrite, conversionOffset));
        } else {
            part.addEntry(bytesToWrite, conversionOffset);
        }

        for (int i = 0; i < numberOfHeaders; i++) {
            writeSize = Math.min(bytesToWrite - writtenBytes, AbstractLogEntryHeader.getMaxLogEntrySize()) - headerSize;

            if (numberOfHeaders > 1) {
                // Log entry is too large and must be chained -> set chaining ID, chain size and length in header for this part
                AbstractPrimLogEntryHeader.addChainingIDAndChainSize(header, 0, (byte) i, (byte) numberOfHeaders, logEntryHeader);
                AbstractPrimLogEntryHeader.adjustLength(header, 0, writeSize, logEntryHeader);
            }

            // Determine free space from end of log to end of array
            if (writePointer >= m_bufferReadPointer) {
                bytesUntilEnd = m_writeBufferSize - writePointer;
            } else {
                bytesUntilEnd = m_bufferReadPointer - writePointer;
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
                        p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(), bytesUntilEnd - headerSize);

                        p_importer.readBytes(m_bufferWrapper.getAddress(), 0, writeSize - (bytesUntilEnd - headerSize));
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
                AbstractPrimLogEntryHeader.addChecksum(m_bufferWrapper, writePointer, writeSize, logEntryHeader, headerSize, bytesUntilEnd);
            }

            writePointer = (writePointer + writeSize + headerSize) % m_buffer.capacity();
            writtenBytes += writeSize + headerSize;
        }

        // ***Synchronization***//
        // Grant writer thread access to meta-data (data available)
        if (bytesInWriteBuffer + bytesToWrite >= SIGNAL_ON_BYTE_COUNT) {
            // Acquires metadata lock -> changed values become visible to writer thread
            grantAccessToWriterThread();
        }

        // Grant writer thread access to meta-data (time-out)
        if (m_writerThreadRequestsAccessToBuffer) {
            m_writerThreadAccessesBuffer = true;
            m_needToLock = true;
        }
    }

    /**
     * Grant the writer thread access to buffer meta-data
     */
    public void grantAccessToWriterThread() {
        m_metadataLock.lock();
        m_writerThreadAccessesBuffer = true;
        m_needToLock = true;

        // Send signal to start flushing by writer thread
        m_dataAvailable = true;
        m_dataAvailableCond.signalAll();
        m_metadataLock.unlock();
    }

    /**
     * Wakes-up writer thread and flushes data to primary log
     * Is only called by exclusive message handler
     */
    public final void signalWriterThreadAndFlushToPrimLog() {
        grantAccessToWriterThread();

        m_flushingComplete = false;
        while (!m_flushingComplete) {
            Thread.yield();
        }
    }

    // Classes

    /**
     * Writer thread The writer thread flushes data from buffer to primary log
     * after being waked-up (signal or timer)
     *
     * @author Kevin Beineke 06.06.2014
     */
    private final class PrimaryLogProcessThread extends Thread {

        // Constructors

        /**
         * Creates an instance of PrimaryLogProcessThread
         */
        PrimaryLogProcessThread() {
        }

        @Override
        public void run() {
            ByteBuffer buffer = m_buffer.duplicate();
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            while (true) {
                if (m_isShuttingDown) {
                    break;
                }

                try {
                    m_metadataLock.lock();
                    // Check if we got a flush request in the meantime
                    if (!m_dataAvailable) {
                        // Wait for flush request
                        if (!m_dataAvailableCond.await(WRITERTHREAD_TIMEOUTTIME, TimeUnit.MILLISECONDS)) {
                            // Time-out -> ask for meta-data access
                            m_writerThreadRequestsAccessToBuffer = true;
                        }
                    }
                    m_metadataLock.unlock();
                } catch (final InterruptedException ignore) {
                    continue;
                }

                flushDataToPrimaryLog(buffer);
                m_logComponent.getReorganizationThread().grantAccessToCurrentLog();
            }
        }

        /**
         * Flushes all data in write buffer to primary log
         */
        void flushDataToPrimaryLog(final ByteBuffer p_primaryWriteBuffer) {
            int writtenBytes = 0;
            int readPointer;
            int bytesInWriteBuffer;
            Set<Entry<Integer, Partitioning>> lengthAndFragmentationByBackupRange;

            // 1. Gain exclusive write access
            // 2. Copy read pointer and counter
            // 3. Set read pointer and reset counter
            // 4. Grant access to write buffer
            // 5. Write buffer to hard drive
            // -> During writing to hard drive the next slot in Write Buffer can be filled

            // Gain exclusive write access:
            // If signaled by message handler the exclusive write access is already hold
            // Else wait for acknowledgement by message handler
            // If after 100ms (default) the access has not been granted (no log message has arrived) -> do it anyway (the exclusive message
            // handler will know, happened before relation not guaranteed but most likely as the message handler acquires and releases a
            // different lock after returning and writer thread waits for a long time)
            final long timeStart = System.currentTimeMillis();
            while (!m_writerThreadAccessesBuffer) {
                m_logComponent.getReorganizationThread().grantAccessToCurrentLog();
                if (System.currentTimeMillis() > timeStart + WRITERTHREAD_TIMEOUTTIME) {
                    break;
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            m_metadataLock.lock();
            // Copy meta-data
            readPointer = m_bufferReadPointer;
            bytesInWriteBuffer = m_bytesInWriteBuffer;
            lengthAndFragmentationByBackupRange = m_lengthAndFragmentationByBackupRange.entrySet();

            m_bufferReadPointer = m_bufferWritePointer;
            m_bytesInWriteBuffer = 0;
            m_lengthAndFragmentationByBackupRange = new HashMap<Integer, Partitioning>();

            // Release access
            m_dataAvailable = false;
            m_writerThreadAccessesBuffer = false;
            m_writerThreadRequestsAccessToBuffer = false;
            m_finishedCopyingCond.signalAll();
            m_metadataLock.unlock();

            if (bytesInWriteBuffer > 0) {
                // Write data to secondary logs or primary log
                try {
                    p_primaryWriteBuffer.position(readPointer);
                    writtenBytes = bufferAndStore(p_primaryWriteBuffer, readPointer, bytesInWriteBuffer, lengthAndFragmentationByBackupRange);
                } catch (final IOException | InterruptedException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not flush data", e);
                    // #endif /* LOGGER >= ERROR */
                }
                if (writtenBytes != bytesInWriteBuffer) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Flush incomplete!");
                    // #endif /* LOGGER >= ERROR */
                }
            }

            m_flushingComplete = true;
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
            //m_primaryLog.appendData(p_buffer, p_buffer.getBuffer().position());
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
        private void appendToBuffer(final ByteBuffer p_primaryWriteBuffer, final int p_offset, final int p_logEntrySize, final int p_bytesUntilEnd,
                final short p_conversionOffset) {
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
                m_bytesFragmentation += m_logSegmentSize - m_bytesWithTruncatedHeaders;
            }

            m_totalBytes += p_length;
            m_bytesWithTruncatedHeaders += p_length - p_conversionOffset + 1;
        }
    }
}
