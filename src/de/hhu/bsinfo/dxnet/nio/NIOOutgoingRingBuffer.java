package de.hhu.bsinfo.dxnet.nio;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.OutgoingRingBuffer;
import de.hhu.bsinfo.utils.ByteBufferHelper;
import de.hhu.bsinfo.utils.UnsafeMemory;

/**
 * Created by nothaas on 7/17/17.
 */
class NIOOutgoingRingBuffer extends OutgoingRingBuffer {

    private ByteBuffer m_sendByteBuffer;
    private long m_bufferAddr;

    NIOOutgoingRingBuffer(final int p_osBufferSize, final AbstractExporterPool p_exporterPool) {
        super(p_exporterPool);

        m_sendByteBuffer = ByteBuffer.allocateDirect(2 * p_osBufferSize);
        m_bufferAddr = ByteBufferHelper.getDirectAddress(m_sendByteBuffer);
        setBuffer(m_bufferAddr, 2 * p_osBufferSize);
    }

    /**
     * Get buffer from outgoing ring buffer.
     *
     * @return the ByteBuffer
     */
    ByteBuffer popFront() {
        long tmp;
        int posBackRelative;
        int posFrontRelative;

        tmp = popFrontShift();
        posBackRelative = (int) (tmp >> 32 & 0x7FFFFFFF);
        posFrontRelative = (int) (tmp & 0x7FFFFFFF);

        m_sendByteBuffer.position(posFrontRelative);
        m_sendByteBuffer.limit(posBackRelative);

        return m_sendByteBuffer;
    }

    /**
     * Write own NodeID directly after connection establishment.
     *
     * @param p_buffer
     *         the byte buffer including own NodeID
     */
    void pushNodeID(final ByteBuffer p_buffer) {
        if (m_posBack.get() == 0) {
            UnsafeMemory.writeByte(m_bufferAddr, p_buffer.get());
            UnsafeMemory.writeByte(m_bufferAddr + 1, p_buffer.get());
            m_posBack.set(2);
        } else {
            throw new IllegalStateException();
        }
    }
}
