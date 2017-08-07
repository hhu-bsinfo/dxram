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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Write interests for a single connection. This keeps track of available data on the outgoing buffer
 * as well as on flow control to tell the (IB) send thread if there is data to send on any connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.08.2017
 */
class IBWriteInterest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterest.class.getSimpleName());

    private final short m_nodeId;
    private AtomicLong m_interestsAvailable;

    /**
     * Constructor
     *
     * @param p_nodeId
     *         Node id of the current node
     */
    IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_interestsAvailable = new AtomicLong(0);
    }

    @Override
    public String toString() {
        long tmp = m_interestsAvailable.get();
        return NodeID.toHexString(m_nodeId) + ", " + (tmp & 0x7FFFFFFF) + ", " + (tmp >> 32);
    }

    /**
     * Get the node id of the interest buffer
     */
    short getNodeId() {
        return m_nodeId;
    }

    /**
     * Add a new data interest
     *
     * @return True if no interest was available before adding this one, false otherwise
     */
    boolean addDataInterest() {
        return m_interestsAvailable.getAndAdd(1) == 0;
    }

    /**
     * Add a new FC interest
     *
     * @return True if no interest was available before adding this one, false otherwise
     */
    boolean addFcInterest() {
        return m_interestsAvailable.getAndAdd(1L << 32) == 0;
    }

    /**
     * Consume all currently available interests (data and FC)
     *
     * @return Long value holding the number of data interests
     * (lower 32-bit) and FC interests (higher 32-bit)
     */
    long consumeInterests() {
        return m_interestsAvailable.getAndSet(0);
    }

    /**
     * Reset all interests, necessary on node disconnect, only
     */
    void reset() {
        m_interestsAvailable.set(0);
    }
}
