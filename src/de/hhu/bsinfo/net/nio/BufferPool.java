package de.hhu.bsinfo.net.nio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by nothaas on 6/12/17.
 */
public final class BufferPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(BufferPool.class.getSimpleName());

    private static final int LARGE_BUFFER_POOL_SIZE = 16;
    private static final int MEDIUM_BUFFER_POOL_SIZE = 16;
    private static final int SMALL_BUFFER_POOL_SIZE = 5 * 1024;
    private static final int LARGE_BUFFER_POOL_FACTOR = 2;
    private static final int MEDIUM_BUFFER_POOL_FACTOR = 4;
    private static final int SMALL_BUFFER_POOL_FACTOR = 32;

    private final int m_osBufferSize;

    private final ArrayList<ByteBuffer> m_largeBufferPool;
    private final ArrayList<ByteBuffer> m_mediumBufferPool;
    private final ArrayList<ByteBuffer> m_smallBufferPool;
    private final ReentrantLock m_bufferPoolLock;

    public BufferPool(final int p_osBufferSize) {
        m_osBufferSize = p_osBufferSize;

        m_largeBufferPool = new ArrayList<ByteBuffer>();
        m_mediumBufferPool = new ArrayList<ByteBuffer>();
        m_smallBufferPool = new ArrayList<ByteBuffer>();
        for (int i = 0; i < LARGE_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool.add(ByteBuffer.allocate(p_osBufferSize / LARGE_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < MEDIUM_BUFFER_POOL_SIZE; i++) {
            m_mediumBufferPool.add(ByteBuffer.allocate(p_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < SMALL_BUFFER_POOL_SIZE; i++) {
            m_smallBufferPool.add(ByteBuffer.allocate(p_osBufferSize / SMALL_BUFFER_POOL_FACTOR));
        }
        m_bufferPoolLock = new ReentrantLock(false);
    }

    public int getOSBufferSize() {
        return m_osBufferSize;
    }

    public ByteBuffer getBuffer() {
        ByteBuffer buffer;

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
            buffer = ByteBuffer.allocate(m_osBufferSize);
        }
        m_bufferPoolLock.unlock();

        return buffer;
    }

    /**
     * Returns the pooled buffer
     *
     * @param p_byteBuffer
     *         the pooled buffer
     */
    public void returnBuffer(final ByteBuffer p_byteBuffer) {
        m_bufferPoolLock.lock();
        p_byteBuffer.clear();
        if (p_byteBuffer.capacity() == m_osBufferSize / LARGE_BUFFER_POOL_FACTOR) {
            if (m_largeBufferPool.size() < LARGE_BUFFER_POOL_SIZE) {
                m_largeBufferPool.add(p_byteBuffer);
            }
        } else if (p_byteBuffer.capacity() == m_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR) {
            if (m_mediumBufferPool.size() < MEDIUM_BUFFER_POOL_SIZE) {
                m_mediumBufferPool.add(p_byteBuffer);
            }
        } else if (p_byteBuffer.capacity() == m_osBufferSize / SMALL_BUFFER_POOL_FACTOR) {
            if (m_smallBufferPool.size() < SMALL_BUFFER_POOL_SIZE) {
                m_smallBufferPool.add(p_byteBuffer);
            }
        }
        m_bufferPoolLock.unlock();
    }
}
