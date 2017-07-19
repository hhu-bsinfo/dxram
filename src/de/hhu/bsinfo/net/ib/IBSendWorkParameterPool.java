package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by nothaas on 7/11/17.
 */
// a pool of direct buffers to enable returning of multiple parameters to jni on send calls from jni
public class IBSendWorkParameterPool {
    private static final int INITIAL_POOL_SIZE = 100;

    private final int m_dataSize;
    private ByteBuffer[] m_pool = new ByteBuffer[INITIAL_POOL_SIZE];

    public IBSendWorkParameterPool(final int p_dataSize) {
        m_dataSize = p_dataSize;
    }

    public ByteBuffer getInstance() {
        int threadId = (int) Thread.currentThread().getId();

        if (threadId > m_pool.length) {
            // Copying without lock might result in lost allocations but this can be ignored
            ByteBuffer[] tmp = new ByteBuffer[m_pool.length + INITIAL_POOL_SIZE];
            System.arraycopy(m_pool, 0, tmp, 0, m_pool.length);
            m_pool = tmp;
        }

        if (m_pool[threadId] == null) {
            m_pool[threadId] = ByteBuffer.allocateDirect(m_dataSize);
            // consider native byte order (most likely little endian)
            m_pool[threadId].order(ByteOrder.nativeOrder());
        }

        return m_pool[threadId];
    }
}
