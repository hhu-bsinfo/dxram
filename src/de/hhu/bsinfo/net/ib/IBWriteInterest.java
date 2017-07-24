package de.hhu.bsinfo.net.ib;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

class IBWriteInterest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterest.class.getSimpleName());

    private final short m_nodeId;
    private AtomicInteger m_dataInterestAvailable;
    private AtomicInteger m_fcInterestAvailable;
    private AtomicBoolean m_interestAcquired;

    IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_dataInterestAvailable = new AtomicInteger(0);
        m_fcInterestAvailable = new AtomicInteger(0);
        m_interestAcquired = new AtomicBoolean(false);
    }

    @Override
    public String toString() {
        return NodeID.toHexString(m_nodeId) + ", " + m_dataInterestAvailable.get() + ", " + m_dataInterestAvailable.get() + ", " + m_interestAcquired.get();
    }

    short getNodeId() {
        return m_nodeId;
    }

    boolean acquire() {
        return m_interestAcquired.compareAndSet(false, true);
    }

    void release() {
        m_interestAcquired.set(false);
    }

    boolean addDataInterest() {
        int ret = m_dataInterestAvailable.getAndIncrement();

        // #if LOGGER == TRACE
        LOGGER.trace("addDataInterest 0x%X, total count %d", m_nodeId, ret + 1);
        // #endif /* LOGGER == TRACE */

        return ret == 0 && m_fcInterestAvailable.get() == 0;
    }

    boolean addFcInterest() {
        int ret = m_fcInterestAvailable.getAndIncrement();

        // #if LOGGER == TRACE
        LOGGER.trace("addFcInterest 0x%X, total count %d", m_nodeId, ret + 1);
        // #endif /* LOGGER == TRACE */

        return ret == 0 && m_dataInterestAvailable.get() == 0;
    }

    int consumeDataInterests() {
        return m_dataInterestAvailable.getAndSet(0);
    }

    int consumeFcInterests() {
        return m_fcInterestAvailable.getAndSet(0);
    }

    // on node disconnect, only
    void reset() {
        m_dataInterestAvailable.set(0);
        m_fcInterestAvailable.set(0);
        m_interestAcquired.set(false);
    }
}
