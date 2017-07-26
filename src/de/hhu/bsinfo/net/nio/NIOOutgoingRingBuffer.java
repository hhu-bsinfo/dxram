package de.hhu.bsinfo.net.nio;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.ExporterPool;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.utils.ByteBufferHelper;
import de.hhu.bsinfo.utils.UnsafeMemory;

/**
 * Created by nothaas on 7/17/17.
 */
class NIOOutgoingRingBuffer extends AbstractOutgoingRingBuffer {

    private final long m_bufferAddr;
    private final int m_bufferSize;
    private ByteBuffer m_sendByteBuffer;

    NIOOutgoingRingBuffer(final int p_osBufferSize) {
        super(p_osBufferSize * 2);

        m_sendByteBuffer = ByteBuffer.allocateDirect(p_osBufferSize * 2);
        m_bufferSize = p_osBufferSize * 2;
        m_bufferAddr = ByteBufferHelper.getDirectAddress(m_sendByteBuffer);
    }

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

    boolean pushNodeID(final ByteBuffer p_buffer) {
        if (m_posBack.get() == 0) {
            UnsafeMemory.writeByte(m_bufferAddr, p_buffer.get());
            UnsafeMemory.writeByte(m_bufferAddr + 1, p_buffer.get());
            m_posBack.set(2);
        } else {
            throw new IllegalStateException();
        }

        return true;
    }

    @Override
    protected void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        AbstractMessageExporter exporter = ExporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }
}
