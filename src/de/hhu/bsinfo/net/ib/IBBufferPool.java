package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IBBufferPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBBufferPool.class.getSimpleName());

    private ArrayList<ByteBuffer> m_pool;
    private int m_bufferSize;
    private int m_pos;
    private int m_poolSize;
    private ReentrantLock m_lock;

    public IBBufferPool(final int p_bufferSize, final int p_poolSize) {
        m_pool = new ArrayList<ByteBuffer>();
        m_bufferSize = p_bufferSize;
        m_pos = p_poolSize - 1;
        m_poolSize = p_poolSize;
        m_lock = new ReentrantLock(false);

        for (int i = 0; i < m_poolSize; i++) {
            m_pool.add(ByteBuffer.allocateDirect(p_bufferSize));
        }
    }

    public ByteBuffer getBuffer() {
        ByteBuffer buffer;

        m_lock.lock();

        if (m_pos >= 0) {
            buffer = m_pool.get(m_pos);
            m_pos--;
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Insufficient pooled incoming buffers. Expanding pool...");
            // #endif /* LOGGER >= WARN */

            buffer = ByteBuffer.allocateDirect(m_bufferSize);

            // ensures that the ArrayList is expanded
            m_pool.add(null);
            m_poolSize++;
        }

        m_lock.unlock();

        return buffer;
    }

    public void returnBuffer(final ByteBuffer p_buffer) {
        m_lock.lock();

        if (m_pos + 1 >= m_poolSize) {
            m_lock.unlock();
            throw new IllegalStateException("Position overflow");
        }

        m_pos++;

        m_pool.set(m_pos, p_buffer);

        m_lock.unlock();
    }

    @Override
    public String toString() {
        return "NIOBufferPool[m_pos " + m_pos + ']';
    }
}
