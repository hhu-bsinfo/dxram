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
    protected final AtomicLong m_posFrontProducer;
    private final AtomicInteger m_posFrontConsumer;

    private final int[] m_uncommittedMessageSizes;
    private final AtomicInteger m_uncommittedSerializations;

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
        m_posFrontConsumer = new AtomicInteger(0);

        m_uncommittedMessageSizes = new int[MAX_CONCURRENT_THREADS];
        m_uncommittedSerializations = new AtomicInteger(0);

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
        return (m_posBack & 0x7FFFFFFFL) == (m_posFrontConsumer.get() & 0x7FFFFFFFL);
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

        m_posBack += p_writtenBytes;

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
        long posFrontUnlimited;
        int posFrontRelative;

        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        // #ifdef STATISTICS
        boolean waited = false;
        // #endif /* STATISTICS */

        // Limit the number of threads serializing messages to the array size for storing message sizes
        while (m_uncommittedSerializations.incrementAndGet() > MAX_CONCURRENT_THREADS) {
            m_uncommittedSerializations.decrementAndGet();

            // Wait for a minimal time (around xx µs). There is no unpark() involved!
            LockSupport.parkNanos(1);
        }

        // Allocate space in ring buffer by incrementing position back
        while (true) {
            posFrontUnlimited = m_posFrontProducer.get(); // We need this value for compareAndSet operation
            posFrontRelative = (int) ((posFrontUnlimited & 0x7FFFFFFF) % m_bufferSize); // posBackRelative must not exceed buffer

            if (p_messageSize <= m_bufferSize) {
                // Small message -> reserve space if message fits in free space
                if ((m_posBack + m_bufferSize & 0x7FFFFFFF) > (posFrontUnlimited + p_messageSize & 0x7FFFFFFF)) {

                    // #ifdef STATISTICS
                    if (waited) {
                        SOP_WAIT_FULL.leave();
                    }
                    // #endif /* STATISTICS */

                    /* Enter serialization area */
                    int pos;
                    if (m_posFrontProducer.compareAndSet(posFrontUnlimited, posFrontUnlimited + p_messageSize + (1L << 32))) {
                        pos = (int) ((posFrontUnlimited >> 32) % MAX_CONCURRENT_THREADS);
                    } else {
                        continue;
                    }

                    serialize(p_message, posFrontRelative, p_messageSize, p_messageSize > m_bufferSize - posFrontRelative /* with overflow */);

                    /* Leave serialization area */
                    if (m_posFrontConsumer.compareAndSet((int) posFrontUnlimited, (int) (posFrontUnlimited + p_messageSize))) {
                        m_uncommittedMessageSizes[pos] = 0;
                        m_uncommittedSerializations.decrementAndGet();

                        // Increase posFrontConsumer for finished threads
                        if (pos != (int) (((m_posFrontProducer.get() >> 32) - 1) % MAX_CONCURRENT_THREADS)) {
                            // At least one thread entered the serialization area after this thread.
                            // If that thread finished the serialization earlier this thread must increase the posFrontConsumer for it (and consecutive threads)
                            int size = p_messageSize;
                            while (pos != (int) (((m_posFrontProducer.get() >> 32) - 1) % MAX_CONCURRENT_THREADS)) { // Get current value for all iterations
                                pos = (pos + 1) % MAX_CONCURRENT_THREADS;
                                int currentSize = m_uncommittedMessageSizes[pos];

                                // Get message size
                                while (currentSize == 0) {
                                    // The following thread is either not finished or the value is not visible yet

                                    if (m_posFrontConsumer.get() != (int) (posFrontUnlimited + size)) {
                                        // The following thread just finished serialization and increased posFrontConsumer -> nothing to do for this thread
                                        break;
                                    }

                                    // Get new value
                                    Thread.yield();
                                    UnsafeHandler.getInstance().getUnsafe().loadFence();
                                    currentSize = m_uncommittedMessageSizes[pos];
                                }
                                if (currentSize == 0) {
                                    break;
                                }

                                size += currentSize;
                                m_uncommittedMessageSizes[pos] = 0;
                                m_uncommittedSerializations.decrementAndGet();
                                m_posFrontConsumer.compareAndSet((int) (posFrontUnlimited + size - currentSize), (int) (posFrontUnlimited + size));
                            }
                        }
                    } else {
                        // Unable to set posFrontConsumer as current value is lower than expected:
                        // another thread entered the serialization area earlier but is not finished yet
                        m_uncommittedMessageSizes[pos] = p_messageSize;
                        UnsafeHandler.getInstance().getUnsafe().storeFence();
                    }
                    break;

                    // Optimistic increase
                    /*if (m_posFrontProducer.compareAndSet(posFrontUnlimited, posFrontUnlimited + p_messageSize)) {
                        // Position back could be set

                        // Serialize complete message into ring buffer
                        serialize(p_message, posFrontRelative, p_messageSize, p_messageSize > m_bufferSize - posFrontRelative /* with overflow *//*);

                        while (!m_posFrontConsumer.compareAndSet(posFrontUnlimited, posFrontUnlimited + p_messageSize)) {
                            Thread.yield();
                        }

                        break;
                    }*/
                    // Try again
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
                // Optimistic increase
                if (m_posFrontProducer.compareAndSet(posFrontUnlimited, posFrontUnlimited + p_messageSize)) {
                    // Position front could be set

                    // Fill free space with message and continue as soon as more space is available
                    serializeLargeMessage(p_message, p_messageSize, (int) posFrontUnlimited, p_pipeOut);

                    break;
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
     * Initialize exporter and write message into native memory.
     *
     * @param p_message
     *         the message to send
     * @param p_start
     *         the start offset
     * @param p_messageSize
     *         the message bytesAvailable
     * @param p_hasOverflow
     *         whether there is an overflow or not
     * @throws NetworkException
     *         if message could not be serialized
     */
    private void serialize(final Message p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {

        // Get exporter (default or overflow). Small messages are written at once, but might be split into two parts if ring buffer overflows during writing.
        MessageExporterCollection exporterCollection = m_exporterPool.getInstance();
        AbstractMessageExporter exporter = exporterCollection.getMessageExporter(p_hasOverflow);
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
    private void serializeLargeMessage(final Message p_message, final int p_messageSize, final int p_oldPosFront, final AbstractPipeOut p_pipeOut)
            throws NetworkException {
        int posFrontUnlimited;
        int posBack = m_posBack & 0x7FFFFFFF;
        int oldPosFrontUnlimited = p_oldPosFront;
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
            startPosition = (int) (oldPosFrontUnlimited & 0x7FFFFFFFL) % m_bufferSize;
            limit = (posBack - 1) % m_bufferSize;
            exporter.setPosition(startPosition);
            exporter.setLimit(limit);
            exporter.setNumberOfWrittenBytes(allWrittenBytes);

            // Serialize as far as possible (from startPosition to limit)
            try {
                p_message.serialize(exporter, p_messageSize);

                // Break if all bytes have been written. Cannot be here if not as ArrayIndexOutOfBoundsException would have been thrown.
                exporterCollection.deleteUnfinishedOperation();
                break;
            } catch (final ArrayIndexOutOfBoundsException ignore) {
            }

            // Update exporter and inform consumer
            int previouslyWrittenBytes = allWrittenBytes;
            allWrittenBytes = exporter.getNumberOfWrittenBytes();

            // Get new values
            posBack = m_posBack & 0x7FFFFFFF;
            posFrontUnlimited = (int) m_posFrontProducer.get();

            // Reserve space for next write
            oldPosFrontUnlimited = posFrontUnlimited;
            m_posFrontConsumer.set(posFrontUnlimited + Math.min(m_bufferSize - ((posFrontUnlimited & 0x7FFFFFFF) - posBack), p_messageSize - allWrittenBytes));

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.enter();
            // #endif /* STATISTICS */

            p_pipeOut.bufferPosted(allWrittenBytes - previouslyWrittenBytes);

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.leave();
            // #endif /* STATISTICS */

            // Wait for consumer to finish writing
            while ((m_posBack & 0x7FFFFFFF) == posBack) {
                LockSupport.parkNanos(100);
            }
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
    protected long popBackShift() {
        int posFront;
        int posFrontRelative;
        int posBack;
        int posBackRelative;

        // #ifdef STATISTICS
        SOP_POP.enter();
        // #endif /* STATISTICS */

        posFront = m_posFrontConsumer.get() & 0x7FFFFFFF;
        posFrontRelative = posFront % m_bufferSize;
        posBack = m_posBack & 0x7FFFFFFF;
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
