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

package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.Version;
import de.hhu.bsinfo.dxutils.hashtable.IntHashTable;

/**
 * Write buffer. Implemented as a ring buffer on a ByteBuffer. The
 * in-memory write-buffer for writing on primary log is cyclic. Similar to a
 * ring buffer all read and write accesses are done by using pointers. All
 * readable bytes are between read and write pointer. Unused bytes between write
 * and read pointer. This class is designed for one producer and one
 * consumer (process thread).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
public class WriteBuffer {

    private static final Logger LOGGER = LogManager.getFormatterLogger(WriteBuffer.class.getSimpleName());

    private static final int WRITE_BUFFER_MAX_SIZE = 1024 * 1024 * 1024;

    private final int m_writeBufferSize;
    private final int m_writeCapacity;
    private final boolean m_useChecksum;
    private final boolean m_native;

    private final DirectByteBufferWrapper m_bufferWrapper;
    private final ByteBuffer m_buffer;
    private final ByteBuffer m_bufferCopy; /* for process thread to access write buffer concurrently */
    private final IntHashTable m_rangeSizeHashTable;

    private final AtomicBoolean m_metadataLock;
    private volatile long m_bufferReadPointer;
    private volatile long m_bufferWritePointer;

    private volatile boolean m_priorityFlush;

    /**
     * Creates an instance of WriteBuffer with user-specific configuration
     *
     * @param p_writeBufferSize
     *         the size of the write buffer
     * @param p_flashPageSize
     *         the size of a flash page
     * @param p_useChecksum
     *         whether checksums are used
     * @param p_writeCapacity
     *         the write capacity when logging large chunks
     */
    WriteBuffer(final int p_writeBufferSize, final int p_flashPageSize, final boolean p_useChecksum,
            final int p_writeCapacity) {
        m_writeBufferSize = p_writeBufferSize;
        m_writeCapacity = p_writeCapacity;
        m_useChecksum = p_useChecksum;

        m_bufferReadPointer = 0;
        m_bufferWritePointer = 0;

        m_metadataLock = new AtomicBoolean(false);
        if (m_writeBufferSize < p_flashPageSize || m_writeBufferSize > WRITE_BUFFER_MAX_SIZE ||
                Integer.bitCount(m_writeBufferSize) != 1) {
            throw new IllegalArgumentException(
                    "Illegal buffer size! Must be 2^x with " + Math.log(p_flashPageSize) / Math.log(2) + " <= x <= 31");
        }
        m_bufferWrapper = new DirectByteBufferWrapper(m_writeBufferSize, false);
        m_buffer = m_bufferWrapper.getBuffer();
        m_bufferCopy = m_buffer.duplicate();
        m_bufferCopy.order(ByteOrder.LITTLE_ENDIAN);
        m_native = m_buffer.isDirect();
        m_rangeSizeHashTable = new IntHashTable();

        LOGGER.trace("Initialized primary write buffer (%d)", m_writeBufferSize);

    }

    /**
     * Cleans the write buffer and resets the pointer
     */
    public final void close() {
        // Nothing to do here
    }

    /**
     * Sets priority flush flag. Write buffer is flushed as soon as possible.
     * Waits until the process thread finished flushing. This might not be the flushing triggered with this method.
     * Thus, checking the condition (which made the flushing necessary) is mandatory.
     */
    void flush() {
        long writePointer = m_bufferWritePointer;
        m_priorityFlush = true;

        while (m_bufferWritePointer == writePointer && getBytesInBuffer() != 0) {
            LockSupport.parkNanos(1);
        }
    }

    /**
     * Sets priority flush flag. Write buffer is flushed as soon as possible.
     * Repeats until there is no more data left for given range. Is used prior to the reorganization.
     */
    void flush(final short p_owner, final short p_range) {
        int currentRangeSize;
        do {
            flush();
            currentRangeSize = m_rangeSizeHashTable.get((p_owner << 16) + p_range);
        } while (currentRangeSize > 0);
    }

    /**
     * Checks whether the write buffer has to be flushed immediately.
     * Resets the priority flush flag.
     *
     * @return whether the write buffer has to be flushed
     */
    boolean needsToBeFlushed() {
        if (m_priorityFlush) {
            m_priorityFlush = false;
            return true;
        }
        return false;
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
     * @param p_version
     *         the version
     * @param p_timestamp
     *         the time since initialization in seconds
     */
    final void putLogData(final AbstractMessageImporter p_importer, final long p_chunkID, final int p_payloadLength,
            final short p_rangeID, final short p_owner, final Version p_version, final int p_timestamp) {
        AbstractPrimLogEntryHeader logEntryHeader;
        byte headerSize;
        int bytesToWrite;
        int numberOfHeaders;
        ByteBuffer header;

        // Combine owner and range ID in an int to be used as a key in hash table
        int combinedRangeID = (p_owner << 16) + p_rangeID;

        // Create log entry header and write it to a pooled buffer
        // -> easier to handle (overflow, chaining, ...) than writing directly into the primary write buffer
        // Checksum and chaining information are added in loop below
        logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
        header = logEntryHeader
                .createLogEntryHeader(p_chunkID, p_payloadLength, p_version, p_rangeID, p_owner, p_timestamp);
        headerSize = (byte) header.limit();

        // Large chunks are split and chained -> there might be more than one header
        numberOfHeaders = p_payloadLength / (AbstractLogEntryHeader.getMaxLogEntrySize() - headerSize);
        if ((p_payloadLength + headerSize) % (AbstractLogEntryHeader.getMaxLogEntrySize() - headerSize) != 0) {
            numberOfHeaders++;
        }
        bytesToWrite = numberOfHeaders * headerSize + p_payloadLength;

        assert WriteBufferTests
                .checkHeader(header, logEntryHeader, p_chunkID, p_version, p_rangeID, p_owner, p_timestamp,
                        numberOfHeaders);

        if (p_payloadLength <= 0) {
            throw new IllegalArgumentException("No payload for log entry!");
        }
        if (p_payloadLength + headerSize > m_writeBufferSize) {
            throw new IllegalArgumentException(
                    "Chunk is too large to log. Maximum chunk size for current configuration is limited by the write " +
                            "buffer size: " + m_writeBufferSize);
        }
        if (p_payloadLength + headerSize > m_writeCapacity) {
            throw new IllegalArgumentException(
                    "Chunk is too large to log. Maximum chunk size for current configuration is limited by the log " +
                            "segment size multiplied with the write queue size: " + m_writeCapacity);
        }
        if (numberOfHeaders > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Chunk is too large to log. Maximum chunk size for current configuration is " +
                            Byte.MAX_VALUE * AbstractLogEntryHeader.getMaxLogEntrySize() + '!');
        }
        if (bytesToWrite > m_writeBufferSize) {
            throw new IllegalArgumentException("Data to write exceeds buffer size!");
        }

        long readPointerAbsolute;
        long writePointerAbsolute;
        while (true) {
            readPointerAbsolute = m_bufferReadPointer;
            writePointerAbsolute = m_bufferWritePointer; // We need this value for the compareAndSet operation
            if (((readPointerAbsolute + m_writeBufferSize & 0x7FFFFFFF) >
                    (writePointerAbsolute + bytesToWrite & 0x7FFFFFFF) ||
                    /* 31-bit overflow in readPointer but not posFront */
                    (readPointerAbsolute + m_writeBufferSize & 0x7FFFFFFF) < readPointerAbsolute &&
                            (writePointerAbsolute + bytesToWrite & 0x7FFFFFFF) > readPointerAbsolute) &&
                    /* too many zones registered? */
                    m_rangeSizeHashTable.size() < BufferPool.getTotalNumberOfBuffers()) {

                append(p_importer, logEntryHeader, header, headerSize, numberOfHeaders, readPointerAbsolute,
                        writePointerAbsolute, bytesToWrite, p_timestamp);

                // Enter critical area by acquiring spin lock
                while (!m_metadataLock.compareAndSet(false, true)) {
                    // Try again
                }

                // Add bytes to write to log of combinedRangeID (optimization for sorting)
                m_rangeSizeHashTable.add(combinedRangeID, bytesToWrite);

                // Set buffer write pointer and byte counter
                m_bufferWritePointer = writePointerAbsolute + bytesToWrite & 0x7FFFFFFF;

                // Leave critical area by resetting spin lock
                m_metadataLock.set(false);

                break;
            } else {
                // There is not enough space to append the log entry -> wait
                m_priorityFlush = true;

                LockSupport.parkNanos(100);
            }
        }
    }

    /**
     * Appends a log entry to the end of the write buffer.
     *
     * @param p_importer
     *         the importer
     * @param p_logEntryHeader
     *         the log entry header
     * @param p_header
     *         the header bytes
     * @param p_headerSize
     *         the header size
     * @param p_numberOfHeaders
     *         the number of headers if the log entry is split
     * @param p_readPointerAbsolute
     *         the read pointer
     * @param p_writePointerAbsolute
     *         the write pointer
     * @param p_bytesToWrite
     *         the number of bytes to write
     * @param p_timestamp
     *         the time since initialization in seconds (for test purposes)
     */
    private void append(final AbstractMessageImporter p_importer, final AbstractPrimLogEntryHeader p_logEntryHeader,
            final ByteBuffer p_header, final int p_headerSize, final int p_numberOfHeaders,
            final long p_readPointerAbsolute, final long p_writePointerAbsolute, final int p_bytesToWrite,
            final int p_timestamp) {
        int readPointer;
        int writePointer;
        int writeSize;
        int writtenBytes = 0;
        int bytesUntilEnd;
        int headerLimit = p_header.limit();

        readPointer = (int) (p_readPointerAbsolute % m_buffer.capacity());
        writePointer = (int) (p_writePointerAbsolute % m_buffer.capacity());
        for (int i = 0; i < p_numberOfHeaders; i++) {
            writeSize =
                    Math.min(p_bytesToWrite - writtenBytes, AbstractLogEntryHeader.getMaxLogEntrySize()) - p_headerSize;

            if (p_numberOfHeaders > 1) {
                // Log entry is too large and must be chained -> set chaining ID, chain size
                // and length in header for this part
                AbstractPrimLogEntryHeader
                        .addChainingIDAndChainSize(p_header, 0, (byte) i, (byte) p_numberOfHeaders, p_logEntryHeader);
                AbstractPrimLogEntryHeader.adjustLength(p_header, 0, writeSize, p_logEntryHeader);
            }

            // Determine free space from end of log to end of array
            if (writePointer >= readPointer) {
                bytesUntilEnd = m_writeBufferSize - writePointer;
            } else {
                bytesUntilEnd = readPointer - writePointer;
            }

            if (writeSize + p_headerSize <= bytesUntilEnd) {
                // Write header
                m_buffer.put(p_header);

                // Write payload
                if (m_native) {
                    p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(), writeSize);
                } else {
                    p_importer.readBytes(m_buffer.array(), m_buffer.position(), writeSize);
                }
                m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
            } else {
                // Twofold cyclic write access
                if (bytesUntilEnd < p_headerSize) {
                    // Write header
                    p_header.limit(bytesUntilEnd);
                    m_buffer.put(p_header);

                    p_header.limit(p_headerSize);
                    m_buffer.position(0);
                    m_buffer.put(p_header);

                    // Write payload
                    if (m_native) {
                        p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(), writeSize);
                    } else {
                        p_importer.readBytes(m_buffer.array(), m_buffer.position(), writeSize);
                    }
                    m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
                } else if (bytesUntilEnd > p_headerSize) {
                    // Write header
                    m_buffer.put(p_header);

                    // Write payload
                    if (m_native) {
                        p_importer.readBytes(m_bufferWrapper.getAddress(), m_buffer.position(),
                                bytesUntilEnd - p_headerSize);

                        p_importer
                                .readBytes(m_bufferWrapper.getAddress(), 0, writeSize - (bytesUntilEnd - p_headerSize));
                    } else {
                        p_importer.readBytes(m_buffer.array(), m_buffer.position(), bytesUntilEnd - p_headerSize);

                        p_importer.readBytes(m_buffer.array(), 0, writeSize - (bytesUntilEnd - p_headerSize));
                    }
                    m_buffer.position((writeSize - (bytesUntilEnd - p_headerSize)) % m_buffer.capacity());
                } else {
                    // Write header
                    m_buffer.put(p_header);

                    // Write payload
                    if (m_native) {
                        p_importer.readBytes(m_bufferWrapper.getAddress(), 0, writeSize);
                    } else {
                        p_importer.readBytes(m_buffer.array(), 0, writeSize);
                    }
                    m_buffer.position((m_buffer.position() + writeSize) % m_buffer.capacity());
                }
            }
            p_header.position(0);
            p_header.limit(headerLimit);

            int checksum = 0;
            if (m_useChecksum) {
                // Determine checksum for payload and add to header
                checksum = AbstractPrimLogEntryHeader
                        .addChecksum(m_bufferWrapper, writePointer, writeSize, p_logEntryHeader, p_headerSize,
                                bytesUntilEnd);
            }

            assert WriteBufferTests
                    .checkWriteAccess(m_bufferWrapper, writePointer, writeSize, bytesUntilEnd, p_header, p_headerSize,
                            checksum, p_timestamp, (byte) i);

            writePointer = (writePointer + writeSize + p_headerSize) % m_buffer.capacity();
            writtenBytes += writeSize + p_headerSize;
        }

    }

    /**
     * Returns the number of available bytes to flush.
     *
     * @return the number of bytes in write buffer
     */
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
     * Returns all metadata necessary for flushing.
     *
     * @return the metadata
     */
    AtomicMetadata getMetadata() {
        AtomicMetadata ret = null;
        int bytesInWriteBuffer;
        int readPointer;
        List<int[]> lengthByBackupRange;

        // Enter critical area by acquiring spin lock
        while (!m_metadataLock.compareAndSet(false, true)) {
            // Try again
        }

        bytesInWriteBuffer = getBytesInBuffer();

        if (bytesInWriteBuffer != 0) {
            readPointer = (int) m_bufferReadPointer % m_buffer.capacity();

            lengthByBackupRange = m_rangeSizeHashTable.convert();
            m_rangeSizeHashTable.clear();

            m_bufferCopy.position(readPointer);

            ret = AtomicMetadata.getInstance(m_bufferCopy, bytesInWriteBuffer, lengthByBackupRange);
        }

        // Leave critical area by resetting spin lock
        m_metadataLock.set(false);

        return ret;
    }

    /**
     * Updates the read pointer after flushing.
     *
     * @param p_flushedBytes
     *         the number of flushed bytes
     */
    void updateMetadata(final long p_flushedBytes) {
        m_bufferReadPointer =
                m_bufferReadPointer + p_flushedBytes & 0x7FFFFFFF; // Read pointer is updated by process thread, only
    }
}
