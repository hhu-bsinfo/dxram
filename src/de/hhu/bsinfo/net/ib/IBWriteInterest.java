package de.hhu.bsinfo.net.ib;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.utils.NodeID;

public class IBWriteInterest {

    private final short m_nodeId;
    private AtomicBoolean m_interestAvailable;
    private AtomicLong m_dataInterestBytes;
    private AtomicLong m_flowControlData;
    private AtomicBoolean m_interestAcquired;

    public IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_interestAvailable = new AtomicBoolean(false);
        m_dataInterestBytes = new AtomicLong(0);
        m_flowControlData = new AtomicLong(0);
        m_interestAcquired = new AtomicBoolean(false);
    }

    public short getNodeId() {
        return m_nodeId;
    }

    public boolean acquire() {
        return m_interestAcquired.compareAndSet(false, true);
    }

    public long getFlowControlData() {
        return m_flowControlData.get();
    }

    public boolean addDataInterests(final long p_val) {
        m_dataInterestBytes.addAndGet(p_val);

        return !m_interestAvailable.getAndSet(true);
    }

    public boolean addFlowControlInterests(final long p_val) {
        m_flowControlData.addAndGet(p_val);

        return !m_interestAvailable.getAndSet(true);
    }

    public boolean consumedInterests(final long p_dataBytes, final long p_flowControlData) {
        boolean avail = false;

        m_interestAvailable.set(false);

        long remainingFlowControl = m_flowControlData.addAndGet(-p_flowControlData);
        long remainingBytes = m_dataInterestBytes.addAndGet(-p_dataBytes);

        if (remainingBytes > 0 || remainingFlowControl > 0) {
            avail = !m_interestAvailable.getAndSet(true);
        }

        m_interestAcquired.set(false);

        return avail;
    }

    // on node disconnect, only
    public void reset() {
        m_interestAvailable.set(false);
        m_dataInterestBytes.set(0);
        m_flowControlData.set(0);
        m_interestAcquired.set(false);
    }

    @Override
    public String toString() {
        return NodeID.toHexString(m_nodeId) + ", " + m_interestAvailable.get() + ", " + m_dataInterestBytes.get() + ", " + m_flowControlData.get() + ", " +
                m_interestAcquired.get();
    }
}
