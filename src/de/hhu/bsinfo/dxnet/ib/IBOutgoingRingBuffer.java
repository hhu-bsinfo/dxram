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

import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.OutgoingRingBuffer;

/**
 * Implementation of the outgoing ring buffer for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.07.2017
 */
class IBOutgoingRingBuffer extends OutgoingRingBuffer {

    /**
     * Constructor
     *
     * @param p_bufferAddr
     *         Unsafe address of the ring buffer
     * @param p_bufferSize
     *         Size of the buffer
     * @param p_exporterPool
     *         Exporter pool instance
     */
    IBOutgoingRingBuffer(final long p_bufferAddr, final int p_bufferSize, final AbstractExporterPool p_exporterPool) {
        super(p_exporterPool);

        setBuffer(p_bufferAddr, p_bufferSize);
    }

    /**
     * Get the next currently available slice of data to send out
     *
     * @return Long holding the current relative position of the front pointer
     * (lower 32-bit) and the relative position of the back pointer
     * (higher 32-bit) of the ring buffer
     */
    long popFront() {
        return popBack();
    }
}
