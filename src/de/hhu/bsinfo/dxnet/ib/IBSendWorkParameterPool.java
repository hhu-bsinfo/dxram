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

package de.hhu.bsinfo.dxnet.ib;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A pool of DirectByteBuffers to allow returning of multiple parameters to JNI on send calls
 * (originating from the send thread living in the jni ibdxnet subsystem). The data passed
 * down to the C/C++ code is mapped to plain structs.
 * Pool one buffer for each thread to avoid allocation and synchronization between threads
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.07.2017
 */
class IBSendWorkParameterPool {
    private static final int INITIAL_POOL_SIZE = 100;

    private final int m_dataSize;
    private ByteBuffer[] m_pool = new ByteBuffer[INITIAL_POOL_SIZE];

    /**
     * Constructor
     *
     * @param p_dataSize
     *         Size of the data/struct to map to C/C++ using DirectByteBuffers
     */
    IBSendWorkParameterPool(final int p_dataSize) {
        m_dataSize = p_dataSize;
    }

    /**
     * Get an instance of a (Direct)ByteBuffer for the current thread
     *
     * @return Allocated (Direct)ByteBuffer
     */
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
