package de.hhu.bsinfo.net.core;

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

    private final short m_destinationNodeID;

    private final int m_flowControlWindowSize;
    private final ReentrantLock m_flowControlCondLock;
    private final Condition m_flowControlCond;

    private int m_unconfirmedBytes;
    private int m_receivedBytes;

    protected AbstractFlowControl(final short p_destinationNodeID, final int p_flowControlWindowSize) {
        m_destinationNodeID = p_destinationNodeID;
        m_flowControlWindowSize = p_flowControlWindowSize;
        m_flowControlCondLock = new ReentrantLock(false);
        m_flowControlCond = m_flowControlCondLock.newCondition();
    }

    protected short getDestinationNodeId() {
        return m_destinationNodeID;
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

    // call when data has to be written to a connection
    void dataToSend(final int p_writtenBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("flowControlDataToSend (%X): %d", m_destinationNodeID, p_writtenBytes);
        // #endif /* LOGGER >= TRACE */

        m_flowControlCondLock.lock();
        while (m_unconfirmedBytes > m_flowControlWindowSize) {
            try {
                if (!m_flowControlCond.await(1000, TimeUnit.MILLISECONDS)) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Flow control message is overdue for node: 0x%X, unconfirmed bytes: %d", m_destinationNodeID, m_unconfirmedBytes);
                    // #endif /* LOGGER >= WARN */
                }
            } catch (final InterruptedException e) { /* ignore */ }
        }
        m_unconfirmedBytes += p_writtenBytes;
        m_flowControlCondLock.unlock();
    }

    // call when data was received on a connection
    void dataReceived(final int p_receivedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("flowControlDataReceived (%X): %d", m_destinationNodeID, p_receivedBytes);
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
        LOGGER.trace("handleFlowControlData (%X): %d", m_destinationNodeID, p_confirmedBytes);
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

        m_flowControlCondLock.lock();
        ret = m_receivedBytes;

        // #if LOGGER >= TRACE
        LOGGER.trace("getAndResetFlowControlData (%X): %d", m_destinationNodeID, ret);
        // #endif /* LOGGER >= TRACE */

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
