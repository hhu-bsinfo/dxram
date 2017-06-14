package de.hhu.bsinfo.ethnet.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by nothaas on 6/9/17.
 */
public abstract class AbstractFlowControl {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractFlowControl.class.getSimpleName());

    private final short m_destinationNodeId;

    private final int m_flowControlWindowSize;
    private final ReentrantLock m_flowControlCondLock;
    private final Condition m_flowControlCond;

    private int m_unconfirmedBytes;
    private int m_receivedBytes;

    protected AbstractFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize) {
        m_destinationNodeId = p_destinationNodeId;
        m_flowControlWindowSize = p_flowControlWindowSize;
        m_flowControlCondLock = new ReentrantLock(false);
        m_flowControlCond = m_flowControlCondLock.newCondition();
    }

    public boolean isCongested() {
        return m_unconfirmedBytes > m_flowControlWindowSize;
    }

    /**
     * Writes flow control data to the connection without delay
     *
     * @throws NetworkException
     *         if message buffer is too small
     */
    public abstract void flowControlWrite() throws NetworkException;

    // call when data was written to a connection
    public void dataSent(final int p_writtenBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("executeFlowControlDataWritten: %d", p_writtenBytes);
        // #endif /* LOGGER >= TRACE */

        m_flowControlCondLock.lock();
        while (m_unconfirmedBytes > m_flowControlWindowSize) {
            try {
                if (!m_flowControlCond.await(1000, TimeUnit.MILLISECONDS)) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Flow control message is overdue for node: 0x%X", m_destinationNodeId);
                    // #endif /* LOGGER >= WARN */
                }
            } catch (final InterruptedException e) { /* ignore */ }
        }
        m_unconfirmedBytes += p_writtenBytes;
        m_flowControlCondLock.unlock();
    }

    // call when data was received on a connection
    public void dataReceived(final int p_receivedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("executeFlowControlDataReceived: %d", p_receivedBytes);
        // #endif /* LOGGER >= TRACE */

        m_flowControlCondLock.lock();
        m_receivedBytes += p_receivedBytes;
        if (m_receivedBytes > m_flowControlWindowSize * 0.8) {
            try {
                flowControlWrite();
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not send flow control message", e);
                // #endif /* LOGGER >= ERROR */
            }
        }
        m_flowControlCondLock.unlock();
    }

    // call when a flow control message was received
    public void handleFlowControlData(final int p_confirmedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("handleFlowControlMessage: %d", p_confirmedBytes);
        // #endif /* LOGGER >= TRACE */

        m_flowControlCondLock.lock();
        m_unconfirmedBytes -= p_confirmedBytes;

        m_flowControlCond.signalAll();
        m_flowControlCondLock.unlock();
    }

    // call when writing flow control data

    /**
     * Get current number of confirmed bytes and reset for flow control
     *
     * @return the number of confirmed bytes
     */
    public int getAndResetFlowControlData() {
        int ret;

        // #if LOGGER >= TRACE
        LOGGER.trace("getAndResetFlowControlBytes");
        // #endif /* LOGGER >= TRACE */

        m_flowControlCondLock.lock();
        ret = m_receivedBytes;

        // Reset received bytes counter
        m_receivedBytes = 0;
        m_flowControlCondLock.unlock();

        return ret;
    }

    @Override
    public String toString() {
        String str;

        m_flowControlCondLock.lock();
        str = "FlowControl[m_flowControlWindowSize " + m_flowControlWindowSize + ", m_unconfirmedBytes " + m_unconfirmedBytes + ", m_receivedBytes " +
                m_receivedBytes + ']';
        m_flowControlCondLock.unlock();

        return str;
    }
}
