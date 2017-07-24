package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

class IBWriteInterestManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterestManager.class.getSimpleName());

    private final IBWriteInterestQueue m_interestQueue;
    private final IBWriteInterest[] m_writeInterests;

    IBWriteInterestManager() {
        m_interestQueue = new IBWriteInterestQueue();
        m_writeInterests = new IBWriteInterest[NodeID.MAX_ID];

        for (int i = 0; i < m_writeInterests.length; i++) {
            m_writeInterests[i] = new IBWriteInterest((short) i);
        }
    }

    void pushBackDataInterest(final short p_nodeId) {
        // #if LOGGER == TRACE
        LOGGER.trace("pushBackDataInterest: 0x%X", p_nodeId);
        // #endif /* LOGGER == TRACE */

        if (m_writeInterests[p_nodeId & 0xFFFF].addDataInterest()) {
            m_interestQueue.pushBack(p_nodeId);
        }
    }

    void pushBackFcInterest(final short p_nodeId) {
        // #if LOGGER == TRACE
        LOGGER.trace("pushBackDataInterest: 0x%X", p_nodeId);
        // #endif /* LOGGER == TRACE */

        if (m_writeInterests[p_nodeId & 0xFFFF].addFcInterest()) {
            m_interestQueue.pushBack(p_nodeId);
        }
    }

    // caller has to manually consume the interests of both data and fc
    short getNextInterests() {
        short nodeId = m_interestQueue.popFront();

        if (nodeId == NodeID.INVALID_ID) {
            return NodeID.INVALID_ID;
        }

        // got interest token but the queue is already acquired
        // put interest back
        if (!m_writeInterests[nodeId & 0xFFFF].acquire()) {
            // force push back, don't lose interest token
            while (!m_interestQueue.pushBack(nodeId)) {
                Thread.yield();
            }

            return NodeID.INVALID_ID;
        }

        return nodeId;
    }

    boolean consumeDataInterests(final short p_nodeId) {
        // consume current interest count
        // everything that's in the buffer up to this point is going to get sent
        return m_writeInterests[p_nodeId & 0xFFFF].consumeDataInterests() > 0;
    }

    boolean consumeFcInterests(final short p_nodeId) {
        // consume current interest count
        // everything that's in the buffer up to this point is going to get sent
        return m_writeInterests[p_nodeId & 0xFFFF].consumeFcInterests() > 0;
    }

    void finishedProcessingInterests(final short p_nodeId) {
        m_writeInterests[p_nodeId & 0xFFFF].release();
    }

    void nodeDisconnected(final short p_nodeId) {
        m_writeInterests[p_nodeId & 0xFFFF].reset();
    }
}
