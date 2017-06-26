package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class IBBufferPool {

    private ByteBuffer[] m_pool;
    private int m_pos;
    private ReentrantLock m_lock;

    public IBBufferPool(final int p_bufferSize, final int p_poolSize) {
        m_pool = new ByteBuffer[p_poolSize];
        m_pos = 0;
        m_lock = new ReentrantLock(false);

        for (int i = 0; i < m_pool.length; i++) {
            m_pool[i] = ByteBuffer.allocateDirect(p_bufferSize);
        }
    }

    public ByteBuffer getBuffer() {
        ByteBuffer buffer = null;

        m_lock.lock();

        if (m_pos < m_pool.length) {
            buffer = m_pool[m_pos++];
        }

        m_lock.unlock();

        return buffer;
    }

    public void returnBuffer(final ByteBuffer p_buffer) {
        m_lock.lock();

        if (m_pos == 0) {
            throw new IllegalStateException("Position underflow");
        }

        m_pool[--m_pos] = p_buffer;

        m_lock.unlock();
    }

    @Override
    public String toString() {
        return "BufferPool[m_pos " + m_pos + ']';
    }
}
