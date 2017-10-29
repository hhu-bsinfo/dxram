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

        m_sendByteBuffer = ByteBuffer.allocateDirect(p_osBufferSize);
        m_bufferAddr = ByteBufferHelper.getDirectAddress(m_sendByteBuffer);
        setBuffer(m_bufferAddr, p_osBufferSize);
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

        tmp = popBackShift();
        posBackRelative = (int) (tmp >> 32 & 0x7FFFFFFF);
        posFrontRelative = (int) (tmp & 0x7FFFFFFF);

        if (posBackRelative == posFrontRelative) {
            return null;
        }

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
        if (m_posFrontProducer.get() == 0) {
            UnsafeMemory.writeByte(m_bufferAddr, p_buffer.get());
            UnsafeMemory.writeByte(m_bufferAddr + 1, p_buffer.get());
            m_posFrontProducer.set(2);
        } else {
            throw new IllegalStateException();
        }
    }
}
