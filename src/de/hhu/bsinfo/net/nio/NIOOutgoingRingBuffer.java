package de.hhu.bsinfo.net.nio;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Created by nothaas on 7/17/17.
 */
public class NIOOutgoingRingBuffer extends AbstractOutgoingRingBuffer {

    private final byte[] m_buffer;
    private ByteBuffer m_sendByteBuffer;

    NIOOutgoingRingBuffer(final int p_osBufferSize) {
        super(p_osBufferSize);

        m_buffer = new byte[p_osBufferSize * 2];
        m_sendByteBuffer = ByteBuffer.wrap(m_buffer);
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
            m_buffer[0] = p_buffer.get();
            m_buffer[1] = p_buffer.get();
            m_posBack.set(2);
        } else {
            throw new IllegalStateException();
        }

        return true;
    }

    protected void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        NIOMessageExporter exporter = (NIOMessageExporter) NIOExporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_buffer);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }
}
