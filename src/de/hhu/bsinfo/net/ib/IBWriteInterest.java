package de.hhu.bsinfo.net.ib;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.utils.NodeID;

class IBWriteInterest {

    private final short m_nodeId;
    private AtomicInteger m_interestAvailable;
    private AtomicBoolean m_interestAcquired;

    IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_interestAvailable = new AtomicInteger(0);
        m_interestAcquired = new AtomicBoolean(false);
    }

    @Override
    public String toString() {
        return NodeID.toHexString(m_nodeId) + ", " + m_interestAvailable.get() + ", " + m_interestAcquired.get();
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

    boolean addInterest() {
        return m_interestAvailable.getAndIncrement() == 0;
    }

    int consumeInterests() {
        return m_interestAvailable.getAndSet(0);
    }

    // on node disconnect, only
    void reset() {
        m_interestAvailable.set(0);
        m_interestAcquired.set(false);
    }
}
