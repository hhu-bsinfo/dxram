package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

public class IBWriteInterestManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterestManager.class.getSimpleName());

    private final IBWriteInterestQueue m_interestQueue;
    private final IBWriteInterest[] m_writeInterests;

    public IBWriteInterestManager() {
        m_interestQueue = new IBWriteInterestQueue();
        m_writeInterests = new IBWriteInterest[NodeID.MAX_ID];

        for (int i = 0; i < m_writeInterests.length; i++) {
            m_writeInterests[i] = new IBWriteInterest((short) i);
        }
    }

    public void pushBackDataInterest(final short p_nodeId, final long p_bytesAvailable) {
        // #if LOGGER == TRACE
        LOGGER.trace("Push back data interest: 0x%X, %d", p_nodeId, p_bytesAvailable);
        // #endif /* LOGGER == TRACE */

        if (m_writeInterests[p_nodeId & 0xFFFF].addDataInterests(p_bytesAvailable)) {
            m_interestQueue.pushBack(p_nodeId);
        }
    }

    public void pushBackFlowControlInterest(final short p_nodeId, final long p_flowControlData) {
        // #if LOGGER == TRACE
        LOGGER.trace("Push back flow control interest: 0x%X, %d", p_nodeId, p_flowControlData);
        // #endif /* LOGGER == TRACE */

        if (m_writeInterests[p_nodeId & 0xFFFF].addFlowControlInterests(p_flowControlData)) {
            m_interestQueue.pushBack(p_nodeId);
        }
    }

    public IBWriteInterest getNextInterest() {
        short nodeId = m_interestQueue.popFront();

        if (nodeId != NodeID.INVALID_ID) {
            // got interest token but the queue is already acquired
            // put interest back
            if (!m_writeInterests[nodeId & 0xFFFF].acquire()) {
                // force push back, don't lose interest token
                while (!m_interestQueue.pushBack(nodeId)) {
                    Thread.yield();
                }

                return null;
            }

            return m_writeInterests[nodeId & 0xFFFF];
        } else {
            return null;
        }
    }

    public void finishedProcessingInterest(final short p_nodeId, final long p_dataWritten, final long p_flowControlWritten) {
        IBWriteInterest interest = m_writeInterests[p_nodeId & 0xFFFF];

        if (interest.consumedInterests(p_dataWritten, p_flowControlWritten)) {
            m_interestQueue.pushBack(p_nodeId);
        }
    }

    public void nodeDisconnected(final short p_nodeId) {
        m_writeInterests[p_nodeId & 0xFFFF].reset();
    }
}
