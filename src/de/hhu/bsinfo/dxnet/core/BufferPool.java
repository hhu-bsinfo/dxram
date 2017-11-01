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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.ByteBufferHelper;

/**
 * Pools incoming buffers (native memory) in different sizes (used for NIO and Loopback)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2017
 */
public final class BufferPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(BufferPool.class.getSimpleName());

    private static final int LARGE_BUFFER_POOL_SIZE = 8;
    private static final int MEDIUM_BUFFER_POOL_SIZE = 256;
    private static final int SMALL_BUFFER_POOL_SIZE = 4096;

    private static final int LARGE_BUFFER_POOL_FACTOR = 16;
    private static final int MEDIUM_BUFFER_POOL_FACTOR = 32;
    private static final int SMALL_BUFFER_POOL_FACTOR = 256;

    private final int m_osBufferSize;

    private final DirectBufferWrapper[] m_largeBufferPool;
    private volatile int m_posFrontLarge;
    private AtomicInteger m_posBackProducerLarge;
    private AtomicInteger m_posBackConsumerLarge;

    private final DirectBufferWrapper[] m_mediumBufferPool;
    private volatile int m_posFrontMedium;
    private AtomicInteger m_posBackProducerMedium;
    private AtomicInteger m_posBackConsumerMedium;

    private final DirectBufferWrapper[] m_smallBufferPool;
    private volatile int m_posFrontSmall;
    private AtomicInteger m_posBackProducerSmall;
    private AtomicInteger m_posBackConsumerSmall;

    /**
     * Creates an instance of BufferPool
     *
     * @param p_osBufferSize
     *         the kernel buffer size
     */
    public BufferPool(final int p_osBufferSize) {
        m_osBufferSize = p_osBufferSize;

        // Initialize ring-buffer for large buffers
        m_posFrontLarge = 0;
        m_posBackProducerLarge = new AtomicInteger(0);
        m_posBackConsumerLarge = new AtomicInteger(0);
        m_largeBufferPool = new DirectBufferWrapper[LARGE_BUFFER_POOL_SIZE];
        for (int i = 0; i < LARGE_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool[i] = new DirectBufferWrapper(p_osBufferSize / LARGE_BUFFER_POOL_FACTOR);
        }

        // Initialize ring-buffer for medium buffers
        m_posFrontMedium = 0;
        m_posBackProducerMedium = new AtomicInteger(0);
        m_posBackConsumerMedium = new AtomicInteger(0);
        m_mediumBufferPool = new DirectBufferWrapper[MEDIUM_BUFFER_POOL_SIZE];
        for (int i = 0; i < MEDIUM_BUFFER_POOL_SIZE; i++) {
            m_mediumBufferPool[i] = new DirectBufferWrapper(p_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR);
        }

        // Initialize ring-buffer for small buffers
        m_posFrontSmall = 0;
        m_posBackProducerSmall = new AtomicInteger(0);
        m_posBackConsumerSmall = new AtomicInteger(0);
        m_smallBufferPool = new DirectBufferWrapper[SMALL_BUFFER_POOL_SIZE];
        for (int i = 0; i < SMALL_BUFFER_POOL_SIZE; i++) {
            m_smallBufferPool[i] = new DirectBufferWrapper(p_osBufferSize / SMALL_BUFFER_POOL_FACTOR);
        }
    }

    /**
     * Return os buffer size
     *
     * @return the buffer size
     */
    public int getOSBufferSize() {
        return m_osBufferSize;
    }

    /**
     * Get a direct memory buffer
     *
     * @return the buffer
     */
    public DirectBufferWrapper getBuffer() {
        DirectBufferWrapper ret;
        int posFront;

        posFront = m_posFrontLarge & 0x7FFFFFFF;
        if ((m_posBackConsumerLarge.get() + LARGE_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
            // Not empty
            ret = m_largeBufferPool[posFront % LARGE_BUFFER_POOL_SIZE];
            m_posFrontLarge++;
            return ret;
        }

        posFront = m_posFrontMedium & 0x7FFFFFFF;
        if ((m_posBackConsumerMedium.get() + MEDIUM_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
            // Not empty
            ret = m_mediumBufferPool[posFront % MEDIUM_BUFFER_POOL_SIZE];
            m_posFrontMedium++;
            return ret;
        }

        posFront = m_posFrontSmall & 0x7FFFFFFF;
        if ((m_posBackConsumerSmall.get() + SMALL_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
            // Not empty
            ret = m_smallBufferPool[posFront % SMALL_BUFFER_POOL_SIZE];
            m_posFrontSmall++;
            return ret;
        }

        // All buffer pools are empty
        // #if LOGGER >= WARN
        LOGGER.warn("Insufficient pooled incoming buffers. Allocating temporary buffer.");
        // #endif /* LOGGER >= WARN */

        return new DirectBufferWrapper(m_osBufferSize);
    }

    /**
     * Returns the pooled buffer
     *
     * @param p_directBufferWrapper
     *         the pooled buffer
     */
    public void returnBuffer(final DirectBufferWrapper p_directBufferWrapper) {
        int posBackSigned;
        int posBack;
        int posFront;

        p_directBufferWrapper.getBuffer().clear();
        if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / LARGE_BUFFER_POOL_FACTOR) {
            // Buffer fits in large buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                // The opposite case cannot happen (practically) as the complete buffer has to be processed before returning the buffer (meanwhile all thread-
                // local copies of posFront have been updated)
                posFront = m_posFrontLarge & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerLarge.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Full
                    break;
                }

                if (m_posBackProducerLarge.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_largeBufferPool[posBack % LARGE_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after message header has been added
                    while (!m_posBackConsumerLarge.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        } else if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR) {
            // Buffer fits in medium buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                // The opposite case cannot happen (practically) as the complete buffer has to be processed before returning the buffer (meanwhile all thread-
                // local copies of posFront have been updated)
                posFront = m_posFrontMedium & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerMedium.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Full
                    break;
                }

                if (m_posBackProducerMedium.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_mediumBufferPool[posBack % MEDIUM_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after message header has been added
                    while (!m_posBackConsumerMedium.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        } else if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / SMALL_BUFFER_POOL_FACTOR) {
            // Buffer fits in small buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                // The opposite case cannot happen (practically) as the complete buffer has to be processed before returning the buffer (meanwhile all thread-
                // local copies of posFront have been updated)
                posFront = m_posFrontSmall & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerSmall.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Full
                    break;
                }

                if (m_posBackProducerSmall.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_smallBufferPool[posBack % SMALL_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after message header has been added
                    while (!m_posBackConsumerSmall.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        }

        // Return without adding the incoming buffer if pool is full or buffer size is incompatible (was created after initialization)
        // #if LOGGER >= WARN
        LOGGER.warn("Could not add incoming buffer because size (%d) does not match or corresponding queue is full!",
                p_directBufferWrapper.getBuffer().capacity());
        // #endif /* LOGGER >= WARN */
    }

    /**
     * Wrapper class for ByteBuffer and off-heap address
     */
    public static class DirectBufferWrapper {

        private ByteBuffer m_buffer;
        private long m_addr;

        /**
         * Creates an instance of DirectBufferWrapper
         *
         * @param p_size
         *         the buffer size
         */
        DirectBufferWrapper(final int p_size) {
            m_buffer = ByteBuffer.allocateDirect(p_size);
            m_addr = ByteBufferHelper.getDirectAddress(m_buffer);
        }

        /**
         * Get the ByteBuffer
         *
         * @return the ByteBuffer
         */
        public ByteBuffer getBuffer() {
            return m_buffer;
        }

        /**
         * Get the off-heap address
         *
         * @return the address
         */
        public long getAddress() {
            return m_addr;
        }
    }
}
