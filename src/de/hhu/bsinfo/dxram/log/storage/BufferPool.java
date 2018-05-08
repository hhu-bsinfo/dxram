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

package de.hhu.bsinfo.dxram.log.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Pools buffers (native memory) in different sizes
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 26.02.2018
 */
public final class BufferPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(BufferPool.class.getSimpleName());

    // If you change these values, consider changing the writer job queue size as well
    private static final int LARGE_BUFFER_POOL_SIZE = 8;
    private static final int MEDIUM_BUFFER_POOL_SIZE = 32;
    static final int SMALL_BUFFER_POOL_SIZE = 64;

    private static final int LARGE_BUFFER_POOL_FACTOR = 1;
    private static final int MEDIUM_BUFFER_POOL_FACTOR = 8;
    private static final int SMALL_BUFFER_POOL_FACTOR = 16;

    private final int m_logSegmentSize;

    private final DirectByteBufferWrapper[] m_largeBufferPool;
    private volatile int m_posFrontLarge;
    private AtomicInteger m_posBackProducerLarge;
    private AtomicInteger m_posBackConsumerLarge;

    private final DirectByteBufferWrapper[] m_mediumBufferPool;
    private volatile int m_posFrontMedium;
    private AtomicInteger m_posBackProducerMedium;
    private AtomicInteger m_posBackConsumerMedium;

    private final DirectByteBufferWrapper[] m_smallBufferPool;
    private volatile int m_posFrontSmall;
    private AtomicInteger m_posBackProducerSmall;
    private AtomicInteger m_posBackConsumerSmall;

    /**
     * Creates an instance of BufferPool
     *
     * @param p_logSegmentSize
     *         the secondary log segment size
     */
    public BufferPool(final int p_logSegmentSize) {
        m_logSegmentSize = p_logSegmentSize;

        // Initialize ring-buffer for large buffers
        if ((LARGE_BUFFER_POOL_SIZE & LARGE_BUFFER_POOL_SIZE - 1) != 0) {
            throw new RuntimeException("Large buffer pool size must be a power of 2!");
        }
        m_posFrontLarge = 0;
        m_posBackProducerLarge = new AtomicInteger(0);
        m_posBackConsumerLarge = new AtomicInteger(0);
        m_largeBufferPool = new DirectByteBufferWrapper[LARGE_BUFFER_POOL_SIZE];
        for (int i = 0; i < LARGE_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool[i] = new DirectByteBufferWrapper(p_logSegmentSize / LARGE_BUFFER_POOL_FACTOR, true);
        }

        // Initialize ring-buffer for medium buffers
        if ((MEDIUM_BUFFER_POOL_SIZE & MEDIUM_BUFFER_POOL_SIZE - 1) != 0) {
            throw new RuntimeException("Medium buffer pool size must be a power of 2!");
        }
        m_posFrontMedium = 0;
        m_posBackProducerMedium = new AtomicInteger(0);
        m_posBackConsumerMedium = new AtomicInteger(0);
        m_mediumBufferPool = new DirectByteBufferWrapper[MEDIUM_BUFFER_POOL_SIZE];
        for (int i = 0; i < MEDIUM_BUFFER_POOL_SIZE; i++) {
            m_mediumBufferPool[i] = new DirectByteBufferWrapper(p_logSegmentSize / MEDIUM_BUFFER_POOL_FACTOR, true);
        }

        // Initialize ring-buffer for small buffers
        if ((SMALL_BUFFER_POOL_SIZE & SMALL_BUFFER_POOL_SIZE - 1) != 0) {
            throw new RuntimeException("Small buffer pool size must be a power of 2!");
        }
        m_posFrontSmall = 0;
        m_posBackProducerSmall = new AtomicInteger(0);
        m_posBackConsumerSmall = new AtomicInteger(0);
        m_smallBufferPool = new DirectByteBufferWrapper[SMALL_BUFFER_POOL_SIZE];
        for (int i = 0; i < SMALL_BUFFER_POOL_SIZE; i++) {
            m_smallBufferPool[i] = new DirectByteBufferWrapper(p_logSegmentSize / SMALL_BUFFER_POOL_FACTOR, true);
        }
    }

    /**
     * Get a direct memory buffer
     *
     * @return the buffer
     */
    public DirectByteBufferWrapper getBuffer(final int p_length) {
        DirectByteBufferWrapper ret;
        int posFront;
        boolean fallThrough = false;

        while (true) {
            if (p_length + 1 > m_logSegmentSize / MEDIUM_BUFFER_POOL_FACTOR) {
                posFront = m_posFrontLarge & 0x7FFFFFFF;
                if ((m_posBackConsumerLarge.get() + LARGE_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
                    // Not empty
                    ret = m_largeBufferPool[posFront % LARGE_BUFFER_POOL_SIZE];
                    m_posFrontLarge++;
                    return ret;
                } else {
                    // #if LOGGER >= DEBUG
                    LOGGER.debug("Insufficient pooled large buffers. Trying medium buffer pool.");
                    // #endif /* LOGGER >= DEBUG */

                    fallThrough = true;
                }
            }

            if (p_length + 1 > m_logSegmentSize / SMALL_BUFFER_POOL_FACTOR || fallThrough) {
                posFront = m_posFrontMedium & 0x7FFFFFFF;
                if ((m_posBackConsumerMedium.get() + MEDIUM_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
                    // Not empty
                    ret = m_mediumBufferPool[posFront % MEDIUM_BUFFER_POOL_SIZE];
                    m_posFrontMedium++;
                    return ret;
                } else {
                    // #if LOGGER >= DEBUG
                    LOGGER.debug("Insufficient pooled medium buffers. Trying small buffer pool.");
                    // #endif /* LOGGER >= DEBUG */
                }
            }

            posFront = m_posFrontSmall & 0x7FFFFFFF;
            if ((m_posBackConsumerSmall.get() + SMALL_BUFFER_POOL_SIZE & 0x7FFFFFFF) != posFront) {
                // Not empty
                ret = m_smallBufferPool[posFront % SMALL_BUFFER_POOL_SIZE];
                m_posFrontSmall++;
                return ret;
            } else {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Insufficient pooled small buffers. Retrying after sleeping shortly.");
                // #endif /* LOGGER >= DEBUG */

                LockSupport.parkNanos(1000);
            }
        }
    }

    /**
     * Returns the pooled buffer
     *
     * @param p_directBufferWrapper
     *         the pooled buffer
     */

    void returnBuffer(final DirectByteBufferWrapper p_directBufferWrapper) {
        int posBackSigned;
        int posBack;
        int posFront;

        p_directBufferWrapper.getBuffer().clear();
        if (p_directBufferWrapper.getBuffer().capacity() == m_logSegmentSize / LARGE_BUFFER_POOL_FACTOR) {
            // Buffer fits in large buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having
                // posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                posFront = m_posFrontLarge & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerLarge.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Looks like we missed an posFront update because this queue cannot be full at this point
                    continue;
                }

                if (m_posBackProducerLarge.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_largeBufferPool[posBack % LARGE_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after
                    // message header has been added
                    while (!m_posBackConsumerLarge.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        }
        if (p_directBufferWrapper.getBuffer().capacity() == m_logSegmentSize / MEDIUM_BUFFER_POOL_FACTOR) {
            // Buffer fits in medium buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having
                // posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                posFront = m_posFrontMedium & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerMedium.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Looks like we missed an posFront update because this queue cannot be full at this point
                    continue;
                }

                if (m_posBackProducerMedium.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_mediumBufferPool[posBack % MEDIUM_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after
                    // message header has been added
                    while (!m_posBackConsumerMedium.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        }
        if (p_directBufferWrapper.getBuffer().capacity() == m_logSegmentSize / SMALL_BUFFER_POOL_FACTOR) {
            // Buffer fits in small buffer pool
            while (true) {
                // PosFront must be read before posBack to avoid missing a posBack update and thus having
                // posBack % BUFFER_SIZE == posFront % BUFFER_SIZE
                posFront = m_posFrontSmall & 0x7FFFFFFF;
                posBackSigned = m_posBackProducerSmall.get();
                posBack = posBackSigned & 0x7FFFFFFF;
                if (posBack == posFront) {
                    // Looks like we missed an posFront update because this queue cannot be full at this point
                    continue;
                }

                if (m_posBackProducerSmall.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    m_smallBufferPool[posBack % SMALL_BUFFER_POOL_SIZE] = p_directBufferWrapper;

                    // First atomic is necessary to synchronize producers, second to inform consumer after
                    // message header has been added
                    while (!m_posBackConsumerSmall.compareAndSet(posBackSigned, posBackSigned + 1)) {
                        // Producer needs to wait for all other submissions prior to this one
                        // (this thread overtook at least one other producer since updating posBackProducer)
                        Thread.yield();
                    }
                    return;
                }
            }
        }

        // #if LOGGER >= DEBUG
        LOGGER.debug("Buffer could not be returned! Size: %d", p_directBufferWrapper.getBuffer().capacity());
        // #endif /* LOGGER >= DEBUG */
    }
}
