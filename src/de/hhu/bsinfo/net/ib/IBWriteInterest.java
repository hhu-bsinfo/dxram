package de.hhu.bsinfo.net.ib;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

class IBWriteInterest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterest.class.getSimpleName());

    private final short m_nodeId;
    private ReentrantLock m_interestLock;
    private int m_dataInterestAvailable;
    private int m_fcInterestAvailable;
    private AtomicBoolean m_interestAcquired;

    IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_interestLock = new ReentrantLock(false);
        m_dataInterestAvailable = 0;
        m_fcInterestAvailable = 0;
        m_interestAcquired = new AtomicBoolean(false);
    }

    @Override
    public String toString() {
        return NodeID.toHexString(m_nodeId) + ", " + m_dataInterestAvailable + ", " + m_dataInterestAvailable + ", " + m_interestAcquired.get();
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
        boolean ret;

        m_interestLock.lock();

        ret = m_dataInterestAvailable == 0 && m_fcInterestAvailable == 0;
        m_dataInterestAvailable++;

        m_interestLock.unlock();

        return ret;
    }

    boolean addFcInterest() {
        boolean ret;

        m_interestLock.lock();

        ret = m_fcInterestAvailable == 0 && m_dataInterestAvailable == 0;
        m_fcInterestAvailable++;

        m_interestLock.unlock();

        return ret;
    }

    // lower 32-bit data, higher fc interests
    long consumeInterests() {
        long ret;

        m_interestLock.lock();

        ret = (long) m_fcInterestAvailable << 32 | m_dataInterestAvailable;
        m_dataInterestAvailable = 0;
        m_fcInterestAvailable = 0;

        m_interestLock.unlock();

        return ret;
    }

    // on node disconnect, only
    void reset() {
        m_interestLock.lock();
        m_dataInterestAvailable = 0;
        m_fcInterestAvailable = 0;
        m_interestLock.unlock();

        m_interestAcquired.set(false);
    }
}
