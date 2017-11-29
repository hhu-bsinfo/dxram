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

import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Queue to keep track of nodes with available interests. The caller has to ensure
 * to not add a single node id multiple times (see interest manager) to ensure
 * connections are processed in a round robin fashion
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.08.2017
 */
class IBWriteInterestQueue {
    private final ReentrantLock m_lock;
    private final short[] m_queue;
    private int m_front;
    private int m_back;

    /**
     * Constructor
     */
    IBWriteInterestQueue() {
        m_lock = new ReentrantLock(false);
        m_queue = new short[NodeID.MAX_ID];
        m_front = 0;
        m_back = 0;
    }

    /**
     * Push a node id to the back of the queue
     *
     * @param p_nodeId
     *         Node id to push back
     * @return True if successful, false if queue full
     */
    public boolean pushBack(final short p_nodeId) {
        if (p_nodeId == NodeID.INVALID_ID) {
            throw new IllegalStateException("Invalid node id is not allowed on interest queue");
        }

        m_lock.lock();

        if ((m_back + 1) % m_queue.length == m_front % m_queue.length) {
            m_lock.unlock();
            return false;
        }

        m_queue[m_back % m_queue.length] = p_nodeId;
        m_back++;

        m_lock.unlock();
        return true;
    }

    /**
     * Pop the next node id from the front
     *
     * @return Valid node id if available, -1 if queue empty
     */
    public short popFront() {
        m_lock.lock();

        if (m_back % m_queue.length == m_front % m_queue.length) {
            m_lock.unlock();
            return NodeID.INVALID_ID;
        }

        short elem = m_queue[m_front % m_queue.length];
        m_front++;

        m_lock.unlock();

        return elem;
    }
}
