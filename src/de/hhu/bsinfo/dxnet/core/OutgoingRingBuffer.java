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

package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import de.hhu.bsinfo.utils.UnsafeHandler;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Lock-free ring buffer implementation for outgoing messages. The implementation allows many threads to
 * serialize their messages to send directly into the buffer. The backends can use this (unsafe)
 * buffer to write the contents directly to the implemented transport without creating any copies.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class OutgoingRingBuffer {
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "Push");
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "Pop");
    private static final StatisticsOperation SOP_WAIT_FULL = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "WaitFull");
    private static final StatisticsOperation SOP_SHIFT_BACK = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "ShiftFront");
    private static final StatisticsOperation SOP_BUFFER_POSTED = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "BufferPosted");

    private static final int MAX_CONCURRENT_THREADS = 1000;

    // multiple producer, single consumer
    private volatile int m_posBack;

    // Upper 32 bits: front position in m_uncommittedMessageSizes; lower 32 bits: current front producer pointer in ring buffer
    protected final AtomicLong m_posFrontProducer;
    // Upper 32 bits: back position in m_uncommittedMessageSizes; lower 32 bits: current front consumer pointer in ring buffer
    protected final AtomicLong m_posFrontConsumer;
    // Also stores the back position in m_uncommittedMessageSizes because back position in m_posFrontConsumer is invalidated during committing
    private final AtomicInteger m_posBackArray;

    private final AtomicBoolean m_largeMessageBeingWritten;

    private final int[] m_uncommittedMessageSizes;
    private long m_bufferAddr;
    private int m_bufferSize;

    private AbstractExporterPool m_exporterPool;

    /**
     * Creates an instance of OutgoingRingBuffer
     *
     * @param p_exporterPool
     *         Pool with exporters to use for serializing messages
     */
    protected OutgoingRingBuffer(final AbstractExporterPool p_exporterPool) {
        m_posBack = 0;
        m_posFrontProducer = new AtomicLong(0);
        m_posFrontConsumer = new AtomicLong(0);

        m_posBackArray = new AtomicInteger(0);

        m_largeMessageBeingWritten = new AtomicBoolean(false);

        m_uncommittedMessageSizes = new int[MAX_CONCURRENT_THREADS];

        m_exporterPool = p_exporterPool;
    }

    /**
     * Set buffer address and bytesAvailable
     *
     * @param p_bufferAddr
     *         the address in native memory
     * @param p_bufferSize
     *         the buffer bytesAvailable
     */
    protected void setBuffer(final long p_bufferAddr, final int p_bufferSize) {
        m_bufferAddr = p_bufferAddr;
        m_bufferSize = p_bufferSize;

        if ((m_bufferSize & m_bufferSize - 1) != 0) {
            throw new NetworkRuntimeException("Outgoing ring-buffer size must be a power of 2!");
        }
    }

    /**
     * Returns the capacity
     *
     * @return the capacity
     */
    public int capacity() {
        return m_bufferSize;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return m_posBack == m_posFrontProducer.get();
    }

    /**
     * Shifts the back after sending data
     *
     * @param p_writtenBytes
     *         the number of written bytes
     */
    public void shiftBack(final int p_writtenBytes) {
        // #ifdef STATISTICS
        SOP_SHIFT_BACK.enter(p_writtenBytes);
        // #endif /* STATISTICS */

        m_posBack = m_posBack + p_writtenBytes & 0x7FFFFFFF;

        // #ifdef STATISTICS
        SOP_SHIFT_BACK.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Serializes a message into the ring buffer.
     *
     * @param p_message
     *         the outgoing message
     * @param p_messageSize
     *         the message's bytesAvailable including message header
     * @param p_pipeOut
     *         Outgoing pipe of the connection this outgoing buffer is assigned to
     * @throws NetworkException
     *         if message could not be serialized
     */
    void pushMessage(final Message p_message, final int p_messageSize, final AbstractPipeOut p_pipeOut) throws NetworkException {
        long posFrontProducer;

        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        // #ifdef STATISTICS
        boolean waited = false;
        // #endif /* STATISTICS */

        // Allocate space in ring buffer by incrementing position back
        while (true) {
            if (p_messageSize <= m_bufferSize) {
                posFrontProducer = m_posFrontProducer.get(); // We need this value for compareAndSet operation

                // Small message -> reserve space if message fits in free space
                int posBack = m_posBack;
                if ((posBack + m_bufferSize & 0x7FFFFFFF) > (posFrontProducer + p_messageSize & 0x7FFFFFFF) ||
                        /* 31-bit overflow in posBack but not posFront */
                        (posBack + m_bufferSize & 0x7FFFFFFF) < posBack && (posFrontProducer + p_messageSize & 0x7FFFFFFF) > posBack) {

                    // #ifdef STATISTICS
                    if (waited) {
                        SOP_WAIT_FULL.leave();
                    }
                    // #endif /* STATISTICS */

                    /* Enter serialization area */
                    long newPosFrontProducer = enterSerializationArea(posFrontProducer, p_messageSize);
                    if (newPosFrontProducer == -1) {
                        continue;
                    }

                    serialize(p_message, ((int) posFrontProducer & 0x7FFFFFFF) % m_bufferSize, p_messageSize);

                    /* Leave serialization area */
                    leaveSerializationArea(posFrontProducer, newPosFrontProducer, p_messageSize);
                    break;
                } else {
                    // #ifdef STATISTICS
                    if (!waited) {
                        SOP_WAIT_FULL.enter();
                        waited = true;
                    }
                    // #endif /* STATISTICS */

                    // Buffer is full -> wait
                    LockSupport.parkNanos(100);
                }
            } else {
                if (m_largeMessageBeingWritten.compareAndSet(false, true)) {
                    posFrontProducer = m_posFrontProducer.get();
                    long newPosFrontProducer = enterSerializationArea(posFrontProducer, p_messageSize);
                    if (newPosFrontProducer == -1) {
                        continue;
                    }

                    // Fill free space with message and continue as soon as more space is available
                    serializeLargeMessage(p_message, p_messageSize, posFrontProducer, p_pipeOut);

                    m_largeMessageBeingWritten.set(false);
                    break;
                } else {
                    LockSupport.parkNanos(100);
                }
                // Try again
            }
        }

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */

        // #ifdef STATISTICS
        SOP_BUFFER_POSTED.enter();
        // #endif /* STATISTICS */

        p_pipeOut.bufferPosted(p_messageSize);

        // #ifdef STATISTICS
        SOP_BUFFER_POSTED.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Enter serialization area (lock-free)
     *
     * @param p_posFrontProducer
     *         the current front position in ring buffer (for producers)
     * @param p_messageSize
     *         the message size
     * @return the new front position in ring-buffer
     */
    private long enterSerializationArea(final long p_posFrontProducer, final int p_messageSize) {
        long newPosFrontProducer = p_posFrontProducer + p_messageSize & 0x7FFFFFFF; // Add message size -> new posFront for producers
        newPosFrontProducer += ((p_posFrontProducer >> 32) + 1 & 0x7FFFFFFF) << 32; // Increment pos -> new posFront in message size array
        if (!m_posFrontProducer.compareAndSet(p_posFrontProducer, newPosFrontProducer)) {
            // Try again
            return -1;
        }
        return newPosFrontProducer;
    }

    /**
     * Leave serialization area (lock-free)
     *
     * @param p_posFrontProducer
     *         the current front position in ring buffer (for producers)
     * @param p_newPosFrontProducer
     *         the new front position in ring buffer (for producers)
     * @param p_messageSize
     *         the message size
     */
    private void leaveSerializationArea(final long p_posFrontProducer, final long p_newPosFrontProducer, final int p_messageSize) {
        /*while (!m_posFrontConsumer.compareAndSet(p_posFrontProducer, p_newPosFrontProducer)) {
            Thread.yield();
        }*/
        int posFrontArray = (int) (p_posFrontProducer >> 32) & 0x7FFFFFFF;
        long newPosFrontProducer = p_newPosFrontProducer;

        // Wait if posBackArray fell too far behind (might be -1 if a serialization is being committed)
        int posBackArray;
        while (true) {
            posBackArray = m_posBackArray.get();
            if (posFrontArray < (posBackArray + MAX_CONCURRENT_THREADS - 1 & 0x7FFFFFFF) ||
                    /* overflow */ (posBackArray + MAX_CONCURRENT_THREADS - 1 & 0x7FFFFFFF) < posBackArray && posBackArray <= posFrontArray) {
                // The registered index is free -> start committing
                break;
            }

            LockSupport.parkNanos(1);
        }

        if (m_posFrontConsumer.compareAndSet(p_posFrontProducer, (newPosFrontProducer & 0x7FFFFFFF) + (-1L << 32))) {
            // Commit serialization
            m_uncommittedMessageSizes[posFrontArray % MAX_CONCURRENT_THREADS] = 0;
            m_posBackArray.set(posFrontArray + 1 & 0x7FFFFFFF);
            m_posFrontConsumer.compareAndSet((newPosFrontProducer & 0x7FFFFFFF) + (-1L << 32), newPosFrontProducer);

            // Increase posFrontConsumer for finished threads
            // At least one thread entered the serialization area after this thread.
            // If that thread finished the serialization earlier this thread must increase the posFrontConsumer for it (and consecutive threads)
            int currentSize;
            while (true) {
                posFrontArray = posFrontArray + 1 & 0x7FFFFFFF;
                UnsafeHandler.getInstance().getUnsafe().loadFence();
                currentSize = m_uncommittedMessageSizes[posFrontArray % MAX_CONCURRENT_THREADS];

                // Get message size
                while (currentSize == 0) {
                    // The following thread is either not finished or the value is not visible yet

                    if (m_posFrontProducer.get() == newPosFrontProducer) {
                        // No following thread -> nothing to do for this thread
                        break;
                    }

                    if (m_posFrontConsumer.get() != newPosFrontProducer) {
                        // The following thread just finished serialization and increased posFrontConsumer -> nothing to do for this thread
                        break;
                    }

                    // Get new value
                    Thread.yield();
                    UnsafeHandler.getInstance().getUnsafe().loadFence();
                    currentSize = m_uncommittedMessageSizes[posFrontArray % MAX_CONCURRENT_THREADS];
                }
                if (currentSize == 0) {
                    break;
                }

                long oldPosFrontProducer = newPosFrontProducer;
                newPosFrontProducer = oldPosFrontProducer + currentSize & 0x7FFFFFFF; // Add message size -> new posFront for producers
                newPosFrontProducer += (long) (posFrontArray + 1 & 0x7FFFFFFF) << 32; // Increment pos -> new posFront in message size array
                if (!m_posFrontConsumer.compareAndSet(oldPosFrontProducer, (newPosFrontProducer & 0x7FFFFFFF) + (-1L << 32))) {
                    // Another thread committed this serialization already -> nothing to do for this thread
                    break;
                }
                m_uncommittedMessageSizes[posFrontArray % MAX_CONCURRENT_THREADS] = 0;
                m_posBackArray.set(posFrontArray + 1 & 0x7FFFFFFF);
                m_posFrontConsumer.compareAndSet((newPosFrontProducer & 0x7FFFFFFF) + (-1L << 32), newPosFrontProducer);
            }
        } else {
            // Unable to set posFrontConsumer as current value is lower than expected:
            // another thread entered the serialization area earlier but is not finished yet
            m_uncommittedMessageSizes[posFrontArray % MAX_CONCURRENT_THREADS] = p_messageSize;
            UnsafeHandler.getInstance().getUnsafe().storeFence();
        }
    }

    /**
     * Initialize exporter and write message into native memory.
     *
     * @param p_message
     *         the message to send
     * @param p_start
     *         the start offset
     * @param p_messageSize
     *         the message bytesAvailable
     * @throws NetworkException
     *         if message could not be serialized
     */
    private void serialize(final Message p_message, final int p_start, final int p_messageSize) throws NetworkException {

        // Get exporter (default or overflow). Small messages are written at once, but might be split into two parts if ring buffer overflows during writing.
        MessageExporterCollection exporterCollection = m_exporterPool.getInstance();
        AbstractMessageExporter exporter = exporterCollection.getMessageExporter(p_messageSize > m_bufferSize - p_start);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }

    /**
     * Initialize exporter and write message iteratively into native memory.
     *
     * @param p_message
     *         the message to send
     * @param p_messageSize
     *         the message bytesAvailable
     * @param p_pipeOut
     *         Outgoing pipe of the connection this outgoing buffer is assigned to
     * @throws NetworkException
     *         if message could not be serialized
     */
    private void serializeLargeMessage(final Message p_message, final int p_messageSize, final long p_oldPosFrontProducer, final AbstractPipeOut p_pipeOut)
            throws NetworkException {
        long posFrontConsumer = p_oldPosFrontProducer;
        int posBack = m_posBack;
        int allWrittenBytes = 0;
        int startPosition;
        int limit;
        MessageExporterCollection exporterCollection;
        LargeMessageExporter exporter;

        // Get exporter pool and large message exporter
        exporterCollection = m_exporterPool.getInstance();
        exporter = exporterCollection.getLargeMessageExporter();
        exporter.setBuffer(m_bufferAddr, m_bufferSize);

        while (true) {
            // Calculate start and limit (might not be reached) for next write and configure exporter
            startPosition = (int) (posFrontConsumer & 0x7FFFFFFFL) % m_bufferSize;
            limit = (posBack - 1) % m_bufferSize;
            if (limit == -1) {
                limit = m_bufferSize - 1;
            }
            exporter.setPosition(startPosition);
            exporter.setLimit(limit);
            exporter.setNumberOfWrittenBytes(allWrittenBytes);

            // Serialize as far as possible (from startPosition to limit)
            try {
                p_message.serialize(exporter, p_messageSize);

                // Break if all bytes have been written. Cannot be here if not as ArrayIndexOutOfBoundsException would have been thrown.
                exporterCollection.deleteUnfinishedOperation();
            } catch (final ArrayIndexOutOfBoundsException ignore) {
            }

            // Update exporter and inform consumer
            int previouslyWrittenBytes = allWrittenBytes;
            allWrittenBytes = exporter.getNumberOfWrittenBytes();

            // Get new values
            posBack = m_posBack;
            posFrontConsumer = (posFrontConsumer & 0xFFFFFFFF00000000L) + ((int) posFrontConsumer + allWrittenBytes - previouslyWrittenBytes & 0x7FFFFFFF);

            // Reserve space for next write
            m_posFrontConsumer.set(posFrontConsumer);

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.enter();
            // #endif /* STATISTICS */

            p_pipeOut.bufferPosted(allWrittenBytes - previouslyWrittenBytes);

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.leave();
            // #endif /* STATISTICS */

            if (p_messageSize == allWrittenBytes) {
                break;
            }

            // Wait for consumer to finish writing
            while (m_posBack == posBack) {
                LockSupport.parkNanos(100);
            }
            posBack = m_posBack;
        }
    }

    /**
     * Get area in ring buffer to send, split in start and end address within ring buffer.
     * This always returns pointers with back < front => if a wrap around is currently available
     * on the buffer, this must be called twice: first call handles front up to end of buffer,
     * second call buffer start up to back
     *
     * @return the start and end address; empty if both back and front are equal
     */
    protected long popBack() {
        int posFront;
        int posFrontRelative;
        int posBack;
        int posBackRelative;

        // #ifdef STATISTICS
        SOP_POP.enter();
        // #endif /* STATISTICS */

        posFront = (int) m_posFrontConsumer.get();
        posFrontRelative = posFront % m_bufferSize;
        posBack = m_posBack;
        posBackRelative = posBack % m_bufferSize;

        if (posFrontRelative < posBackRelative) {
            posFrontRelative = m_bufferSize;
        }

        // #ifdef STATISTICS
        SOP_POP.leave();
        // #endif /* STATISTICS */

        return (long) posFrontRelative << 32 | (long) posBackRelative;
    }
}
