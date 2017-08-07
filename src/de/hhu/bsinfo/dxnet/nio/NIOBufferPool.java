package de.hhu.bsinfo.dxnet.nio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.ByteBufferHelper;

/**
 * Created by nothaas on 6/12/17.
 */
public final class NIOBufferPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOBufferPool.class.getSimpleName());

    private static final int LARGE_BUFFER_POOL_SIZE = 16;
    private static final int MEDIUM_BUFFER_POOL_SIZE = 16;
    private static final int SMALL_BUFFER_POOL_SIZE = 5 * 1024;
    private static final int LARGE_BUFFER_POOL_FACTOR = 2;
    private static final int MEDIUM_BUFFER_POOL_FACTOR = 4;
    private static final int SMALL_BUFFER_POOL_FACTOR = 32;

    private final int m_osBufferSize;

    private final ArrayList<DirectBufferWrapper> m_largeBufferPool;
    private final ArrayList<DirectBufferWrapper> m_mediumBufferPool;
    private final ArrayList<DirectBufferWrapper> m_smallBufferPool;
    private final ReentrantLock m_bufferPoolLock;

    NIOBufferPool(final int p_osBufferSize) {
        m_osBufferSize = p_osBufferSize;

        m_largeBufferPool = new ArrayList<DirectBufferWrapper>();
        m_mediumBufferPool = new ArrayList<DirectBufferWrapper>();
        m_smallBufferPool = new ArrayList<DirectBufferWrapper>();
        for (int i = 0; i < LARGE_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool.add(new DirectBufferWrapper(p_osBufferSize / LARGE_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < MEDIUM_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool.add(new DirectBufferWrapper(p_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < SMALL_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool.add(new DirectBufferWrapper(p_osBufferSize / SMALL_BUFFER_POOL_FACTOR));
        }
        m_bufferPoolLock = new ReentrantLock(false);
    }

    int getOSBufferSize() {
        return m_osBufferSize;
    }

    public DirectBufferWrapper getBuffer() {
        DirectBufferWrapper buffer;

        m_bufferPoolLock.lock();
        if (!m_largeBufferPool.isEmpty()) {
            buffer = m_largeBufferPool.remove(m_largeBufferPool.size() - 1);
        } else if (!m_mediumBufferPool.isEmpty()) {
            buffer = m_mediumBufferPool.remove(m_mediumBufferPool.size() - 1);
        } else if (!m_smallBufferPool.isEmpty()) {
            buffer = m_smallBufferPool.remove(m_smallBufferPool.size() - 1);
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Insufficient pooled incoming buffers. Allocating temporary buffer.");
            // #endif /* LOGGER >= WARN */

            buffer = new DirectBufferWrapper(m_osBufferSize);
        }
        m_bufferPoolLock.unlock();

        return buffer;
    }

    /**
     * Returns the pooled buffer
     *
     * @param p_directBufferWrapper
     *         the pooled buffer
     */
    void returnBuffer(final DirectBufferWrapper p_directBufferWrapper) {
        m_bufferPoolLock.lock();
        p_directBufferWrapper.getBuffer().clear();
        if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / LARGE_BUFFER_POOL_FACTOR) {
            if (m_largeBufferPool.size() < LARGE_BUFFER_POOL_SIZE) {
                m_largeBufferPool.add(p_directBufferWrapper);
            }
        } else if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR) {
            if (m_mediumBufferPool.size() < MEDIUM_BUFFER_POOL_SIZE) {
                m_mediumBufferPool.add(p_directBufferWrapper);
            }
        } else if (p_directBufferWrapper.getBuffer().capacity() == m_osBufferSize / SMALL_BUFFER_POOL_FACTOR) {
            if (m_smallBufferPool.size() < SMALL_BUFFER_POOL_SIZE) {
                m_smallBufferPool.add(p_directBufferWrapper);
            }
        }
        m_bufferPoolLock.unlock();
    }

    public static class DirectBufferWrapper {

        private ByteBuffer m_buffer;
        private long m_addr;

        DirectBufferWrapper(final int p_size) {
            m_buffer = ByteBuffer.allocateDirect(p_size);
            m_addr = ByteBufferHelper.getDirectAddress(m_buffer);
        }

        ByteBuffer getBuffer() {
            return m_buffer;
        }

        long getAddress() {
            return m_addr;
        }
    }
}
