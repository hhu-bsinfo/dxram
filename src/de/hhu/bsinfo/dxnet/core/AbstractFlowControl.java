package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by nothaas on 6/9/17.
 */
public abstract class AbstractFlowControl {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractFlowControl.class.getSimpleName());

    private final short m_destinationNodeID;

    private final int m_flowControlWindowSize;
    private final float m_flowControlWindowThreshold;

    private AtomicInteger m_unconfirmedBytes;
    private AtomicInteger m_receivedBytes;

    protected AbstractFlowControl(final short p_destinationNodeID, final int p_flowControlWindowSize, final float p_flowControlWindowThreshold) {
        m_destinationNodeID = p_destinationNodeID;
        m_flowControlWindowSize = p_flowControlWindowSize;
        m_flowControlWindowThreshold = p_flowControlWindowThreshold;

        m_unconfirmedBytes = new AtomicInteger(0);
        m_receivedBytes = new AtomicInteger(0);
    }

    protected short getDestinationNodeId() {
        return m_destinationNodeID;
    }

    public boolean isCongested() {
        return m_unconfirmedBytes.get() > m_flowControlWindowSize;
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

        while (m_unconfirmedBytes.get() > m_flowControlWindowSize) {
            LockSupport.parkNanos(1);
        }

        m_unconfirmedBytes.addAndGet(p_writtenBytes);
    }

    // call when data was received on a connection
    void dataReceived(final int p_receivedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("flowControlDataReceived (%X): %d", m_destinationNodeID, p_receivedBytes);
        // #endif /* LOGGER >= TRACE */

        int receivedBytes = m_receivedBytes.addAndGet(p_receivedBytes);
        if (receivedBytes > m_flowControlWindowSize * m_flowControlWindowThreshold) {
            try {
                flowControlWrite();
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not send flow control message", e);
                // #endif /* LOGGER >= ERROR */
            }
        }
    }

    // call when a flow control message was received
    public void handleFlowControlData(final int p_confirmedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("handleFlowControlData (%X): %d", m_destinationNodeID, p_confirmedBytes);
        // #endif /* LOGGER >= TRACE */

        m_unconfirmedBytes.addAndGet(-p_confirmedBytes);
    }

    // call when writing flow control data

    /**
     * Get current number of confirmed bytes and reset for flow control
     *
     * @return the number of confirmed bytes
     */
    public int getAndResetFlowControlData() {
        int ret;

        ret = m_receivedBytes.getAndSet(0);

        // #if LOGGER >= TRACE
        LOGGER.trace("getAndResetFlowControlData (%X): %d", m_destinationNodeID, ret);
        // #endif /* LOGGER >= TRACE */

        return ret;
    }

    @Override
    public String toString() {
        String str;

        str = "FlowControl[m_flowControlWindowSize " + m_flowControlWindowSize + ", m_unconfirmedBytes " + m_unconfirmedBytes + ", m_receivedBytes " +
                m_receivedBytes + ']';

        return str;
    }
}
