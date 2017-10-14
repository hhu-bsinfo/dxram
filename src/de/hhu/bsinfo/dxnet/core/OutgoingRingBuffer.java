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
import java.util.concurrent.locks.LockSupport;

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
    private static final StatisticsOperation SOP_SHIFT_FRONT = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "ShiftFront");
    private static final StatisticsOperation SOP_BUFFER_POSTED = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "BufferPosted");

    // multiple producer, single consumer
    private volatile int m_posFront;
    protected final AtomicInteger m_posBack;

    private long m_bufferAddr;
    private int m_bufferSize;

    private final AtomicBoolean m_largeMessageInProgress;

    private final AtomicInteger m_producers;
    private volatile boolean m_consumerWaits;

    private AbstractExporterPool m_exporterPool;

    /**
     * Creates an instance of OutgoingRingBuffer
     *
     * @param p_exporterPool
     *         Pool with exporters to use for serializing messages
     */
    protected OutgoingRingBuffer(final AbstractExporterPool p_exporterPool) {
        m_posFront = 0;
        m_posBack = new AtomicInteger(0);

        m_largeMessageInProgress = new AtomicBoolean(false);
        m_producers = new AtomicInteger(0);
        m_consumerWaits = false;

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
        return (m_posFront & 0x7FFFFFFFL) == (m_posBack.get() & 0x7FFFFFFFL);
    }

    /**
     * Shifts the front after sending data
     *
     * @param p_writtenBytes
     *         the number of written bytes
     */
    public void shiftFront(final int p_writtenBytes) {
        // #ifdef STATISTICS
        SOP_SHIFT_FRONT.enter(p_writtenBytes);
        // #endif /* STATISTICS */

        m_posFront += p_writtenBytes;

        // #ifdef STATISTICS
        SOP_SHIFT_FRONT.leave();
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
        int posBackUnlimited;
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;

        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        m_producers.incrementAndGet();
        while (m_consumerWaits) {
            // Consumer wants to flush
            m_producers.decrementAndGet();
            while (m_consumerWaits) {
                // Wait for a minimal time (around xx µs). There is no unpark() involved!
                //LockSupport.parkNanos(1);
                Thread.yield();
            }
            m_producers.incrementAndGet();
        }

        // #ifdef STATISTICS
        boolean waited = false;
        // #endif /* STATISTICS */

        // Allocate space in ring buffer by incrementing position back
        while (true) {
            posBackUnlimited = m_posBack.get(); // We need this value for compareAndSet operation
            posBack = (int) (posBackUnlimited & 0x7FFFFFFFL); // posBack must not be negative
            posBackRelative = posBack % m_bufferSize; // posBackRelative must not exceed buffer
            posFront = (int) (m_posFront & 0x7FFFFFFFL);
            posFrontRelative = posFront % m_bufferSize;

            if (p_messageSize <= m_bufferSize) {
                // Small message -> reserve space if message fits in free space
                if (!m_largeMessageInProgress.get() && fits(posBackRelative, posFrontRelative, p_messageSize)) {

                    // #ifdef STATISTICS
                    if (waited) {
                    SOP_WAIT_FULL.leave();
                    }
                    // #endif /* STATISTICS */

                    // Optimistic increase
                    if (m_posBack.compareAndSet(posBackUnlimited, posBackUnlimited + p_messageSize)) {
                        // Position back could be set

                        // Serialize complete message into ring buffer
                        serialize(p_message, posBackRelative, p_messageSize, p_messageSize > m_bufferSize - posBackRelative /* with overflow */);

                        break;
                    }
                    // Try again
                } else {
                    // #ifdef STATISTICS
                    if (!waited) {
                    SOP_WAIT_FULL.enter();
                    waited = true;
                    }
                    // #endif /* STATISTICS */

                    // Buffer is full -> wait
                    m_producers.decrementAndGet();

                    LockSupport.parkNanos(100);

                    m_producers.incrementAndGet();
                }
            } else {
                // Large messages -> reserve all space available
                if (m_largeMessageInProgress.compareAndSet(false, true)) {

                    // Optimistic increase
                    if (m_posBack.compareAndSet(posBackUnlimited, posBackUnlimited + bytesAvailable(posBackRelative, posFrontRelative))) {
                        // Position back could be set

                        // Fill free space with message and continue as soon as more space is available
                        serializeLargeMessage(p_message, p_messageSize, posBackUnlimited, p_pipeOut);

                        break;
                    }
                } else {
                    // A large message is being written already -> wait
                    m_producers.decrementAndGet();

                    LockSupport.parkNanos(100);

                    m_producers.incrementAndGet();
                }
                // Try again
            }
        }

        // Leave
        m_producers.decrementAndGet();

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
    private void serializeLargeMessage(final Message p_message, final int p_messageSize, final int p_oldPosBack, final AbstractPipeOut p_pipeOut)
            throws NetworkException {
        int posBackUnlimited;
        int posBackRelative;
        int posFront = (int) (m_posFront & 0x7FFFFFFFL);
        int posFrontRelative;
        int oldPosBackUnlimited = p_oldPosBack;
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
            startPosition = (int) (oldPosBackUnlimited & 0x7FFFFFFFL) % m_bufferSize;
            limit = (posFront - 1) % m_bufferSize;
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

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.enter();
            // #endif /* STATISTICS */

            p_pipeOut.bufferPosted(allWrittenBytes - previouslyWrittenBytes);

            // #ifdef STATISTICS
            SOP_BUFFER_POSTED.leave();
            // #endif /* STATISTICS */

            // Buffer is full now - > wait for consumer
            m_producers.decrementAndGet();

            // Wait for consumer to finish writing
            while ((int) (m_posFront & 0x7FFFFFFFL) == posFront) {
                LockSupport.parkNanos(100);
            }

            // Synchronize with consumer
            m_producers.incrementAndGet();
            while (m_consumerWaits) {
                m_producers.decrementAndGet();
                while (m_consumerWaits) {
                    Thread.yield();
                }
                m_producers.incrementAndGet();
            }

            // Get new values
            posFront = (int) (m_posFront & 0x7FFFFFFFL);
            posFrontRelative = posFront % m_bufferSize;
            posBackUnlimited = m_posBack.get();
            posBackRelative = (int) (posBackUnlimited & 0x7FFFFFFFL) % m_bufferSize;

            // Reserve space for next write
            oldPosBackUnlimited = posBackUnlimited;
            m_posBack.set(posBackUnlimited + Math.min(bytesAvailable(posBackRelative, posFrontRelative), p_messageSize - allWrittenBytes));
        }

        m_largeMessageInProgress.set(false);
    }

    /**
     * Get area in ring buffer to send, split in start and end address within ring buffer.
     * This always returns pointers with front < back => if a wrap around is currently available
     * on the buffer, this must be called twice: first call handles front up to end of buffer,
     * second call buffer start up to back
     *
     * @return the start and end address; empty if both back and front are equal
     */
    protected long popFrontShift() {
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;

        // #ifdef STATISTICS
        SOP_POP.enter();
        // #endif /* STATISTICS */

        // Deny access to incoming producers
        m_consumerWaits = true;

        // Wait for all producers to finish copying
        while (m_producers.get() > 0) {
            Thread.yield();
        }

        posBack = (int) (m_posBack.get() & 0x7FFFFFFFL);
        posBackRelative = posBack % m_bufferSize;
        posFront = (int) (m_posFront & 0x7FFFFFFFL);
        posFrontRelative = posFront % m_bufferSize;

        if (posBackRelative < posFrontRelative) {
            posBackRelative = m_bufferSize;
        }

        m_consumerWaits = false;

        // #ifdef STATISTICS
        SOP_POP.leave();
        // #endif /* STATISTICS */

        return (long) posBackRelative << 32 | (long) posFrontRelative;
    }

    /**
     * Helper method to determine if given message fits
     *
     * @param p_posBackRelative
     *         relative offset back
     * @param p_posFrontRelative
     *         relative offset front
     * @param p_messageSize
     *         the message bytesAvailable
     * @return whether the message fits or not
     */
    private boolean fits(final int p_posBackRelative, final int p_posFrontRelative, final int p_messageSize) {
        // Compare with > instead of >= to let one byte unused at the end in order to avoid posBackRelative == posFrontRelative with posBack != posFront
        return p_posBackRelative >= p_posFrontRelative && m_bufferSize - p_posBackRelative + p_posFrontRelative > p_messageSize /* without overflow */ ||
                p_posBackRelative < p_posFrontRelative && p_posFrontRelative - p_posBackRelative > p_messageSize /* with overflow */;
    }

    /**
     * Helper method to determine free space
     *
     * @param p_posBackRelative
     *         relative offset back
     * @param p_posFrontRelative
     *         relative offset front
     * @return whether the message fits or not
     */
    private int bytesAvailable(final int p_posBackRelative, final int p_posFrontRelative) {
        // -1 in both cases to let one byte unused at the end in order to avoid posBackRelative == posFrontRelative with posBack != posFront
        if (p_posBackRelative >= p_posFrontRelative) {
            // Without overflow
            return m_bufferSize - p_posBackRelative + p_posFrontRelative - 1;
        } else {
            // With overflow
            return p_posFrontRelative - p_posBackRelative - 1;
        }
    }
}
