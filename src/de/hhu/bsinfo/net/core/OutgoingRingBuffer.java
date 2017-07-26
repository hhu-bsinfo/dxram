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

package de.hhu.bsinfo.net.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class OutgoingRingBuffer {
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "RingBufferPush");
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(OutgoingRingBuffer.class, "RingBufferPop");

    // Attributes
    private volatile int m_posFront;
    protected final AtomicInteger m_posBack;

    private long m_bufferAddr;
    private int m_bufferSize;

    private final AtomicInteger m_producers;
    private volatile boolean m_consumerWaits;

    private AbstractExporterPool m_exporterPool;

    /**
     * Creates an instance of OutgoingRingBuffer
     */
    protected OutgoingRingBuffer(final AbstractExporterPool p_exporterPool) {
        m_posFront = 0;
        m_posBack = new AtomicInteger(0);

        m_producers = new AtomicInteger(0);
        m_consumerWaits = false;

        m_exporterPool = p_exporterPool;
    }

    /**
     * Set buffer address and size
     *
     * @param p_bufferAddr
     *         the address in native memory
     * @param p_bufferSize
     *         the buffer size
     */
    protected void setBuffer(final long p_bufferAddr, final int p_bufferSize) {
        m_bufferAddr = p_bufferAddr;
        m_bufferSize = p_bufferSize;
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
        m_posFront += p_writtenBytes;
    }

    /**
     * Serializes a message into the ring buffer.
     *
     * @param p_message
     *         the outgoing message
     * @param p_messageSize
     *         the message's size including message header
     * @throws NetworkException
     *         if message could not be serialized
     */
    void pushMessage(final Message p_message, final int p_messageSize) throws NetworkException {
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
                LockSupport.parkNanos(1);
            }
            m_producers.incrementAndGet();
        }

        // Allocate space in ring buffer by incrementing position back
        while (true) {
            posBackUnlimited = m_posBack.get(); // We need this value for compareAndSet operation
            posBack = (int) (posBackUnlimited & 0x7FFFFFFFL); // posBack must not be negative
            posBackRelative = posBack % m_bufferSize; // posBackRelative must not exceed buffer
            posFront = (int) (m_posFront & 0x7FFFFFFFL);
            posFrontRelative = posFront % m_bufferSize;

            if (p_messageSize <= m_bufferSize) {
                // Small message -> reserve space if message fits in free space
                if (posBack == posFront /* empty */ || !multiOverflow(posBack, posFront) && fits(posBackRelative, posFrontRelative, p_messageSize)) {
                    // Optimistic increase
                    if (m_posBack.compareAndSet(posBackUnlimited, posBackUnlimited + p_messageSize)) {
                        // Position back could be set
                        break;
                    }
                    // Try again
                } else {
                    // Buffer is full -> wait
                    m_producers.decrementAndGet();

                    LockSupport.parkNanos(1);

                    m_producers.incrementAndGet();
                }
            } else {
                // Large messages -> reserve space for complete message to avoid interruption
                // Optimistic increase
                if (m_posBack.compareAndSet(posBackUnlimited, posBackUnlimited + p_messageSize)) {
                    // Position back could be set
                    break;
                }
                // Try again
            }
        }

        if (p_messageSize <= m_bufferSize) {
            // Small message ->  serialize complete message into ring buffer
            serialize(p_message, posBackRelative, p_messageSize, p_messageSize > m_bufferSize - posBackRelative /* with overflow */);
        } else {
            // Large message -> fill free space with message and continue as soon as more space is available

            // TODO: Serialize iteratively

            // TODO: Release "lock" if buffer is full and acquire it to continue
        }

        // Leave
        m_producers.decrementAndGet();

        // #ifdef STATISTICS
        SOP_PUSH.leave();
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
     *         the message size
     * @param p_hasOverflow
     *         whether there is an overflow or not
     * @throws NetworkException
     *         if message could not be serialized
     */
    protected void serialize(final Message p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        AbstractMessageExporter exporter = m_exporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
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
     * Helper method to determine free space. Multiple overflow occurs for large messages.
     *
     * @param p_posBack
     *         absolute unsigned int (31 bit) offset back
     * @param p_posFront
     *         absolute unsigned int (31 bit) offset front
     * @return whether there is a multiple overflow or not
     */
    private boolean multiOverflow(final int p_posBack, final int p_posFront) {
        return p_posBack > p_posFront + m_bufferSize || p_posBack < p_posFront && 0x7FFFFFFFL - p_posFront + p_posBack > m_bufferSize;
    }

    /**
     * Helper method to determine free space
     *
     * @param p_posBackRelative
     *         relative offset back
     * @param p_posFrontRelative
     *         relative offset front
     * @param p_messageSize
     *         the message size
     * @return whether the message fits or not
     */
    private boolean fits(final int p_posBackRelative, final int p_posFrontRelative, final int p_messageSize) {
        return p_posBackRelative > p_posFrontRelative && m_bufferSize - p_posBackRelative + p_posFrontRelative > p_messageSize /* without overflow */ ||
                p_posBackRelative < p_posFrontRelative && p_posFrontRelative - p_posBackRelative > p_messageSize /* with overflow */;
    }
}
