package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Software flow control. Avoids that the sender is flooding the receiver if the receiver can't
 * keep up with processing the incoming buffers, deserializing and distpatching the messages.
 * Flow control confirmation of a full message is sent after it is received and fully processed
 * by the application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.06.2017
 */
public abstract class AbstractFlowControl {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractFlowControl.class.getSimpleName());

    private final short m_destinationNodeID;

    private final int m_flowControlWindowSize;
    private final float m_flowControlWindowThreshold;

    private AtomicInteger m_unconfirmedBytes;
    private AtomicInteger m_receivedBytes;

    /**
     * Constructor
     *
     * @param p_destinationNodeID
     *         Node id of the destination this unit is connected to
     * @param p_flowControlWindowSize
     *         FC window in bytes. When exceeded send out a flow control message
     * @param p_flowControlWindowThreshold
     *         Threshold parameter to adjust the FC window to control when a flow control message is sent
     */
    protected AbstractFlowControl(final short p_destinationNodeID, final int p_flowControlWindowSize, final float p_flowControlWindowThreshold) {
        m_destinationNodeID = p_destinationNodeID;
        m_flowControlWindowSize = p_flowControlWindowSize;
        m_flowControlWindowThreshold = p_flowControlWindowThreshold;

        m_unconfirmedBytes = new AtomicInteger(0);
        m_receivedBytes = new AtomicInteger(0);
    }

    /**
     * Get the destination node id the flow control is connected to
     */
    protected short getDestinationNodeId() {
        return m_destinationNodeID;
    }

    /**
     * Writes flow control data to the destination ASAP (really, do it ASAP or you risk running into ugly deadlocking issues)
     *
     * @throws NetworkException
     *         If writing the flow control data failed
     */
    public abstract void flowControlWrite() throws NetworkException;

    /**
     * Called when messages ("unconfirmed bytes") are written to the connection of the destination
     *
     * @param p_writtenBytes
     *         Number of bytes that were written to the destination
     */
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

    /**
     * Called when messages ("bytes to be confirmed") are received on the remote
     *
     * @param p_receivedBytes
     *         Number of bytes received
     */
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

    // call when writing flow control data

    /**
     * Get current number of "confirmed bytes" to send back to the source (to confirm data was processed)
     * and reset
     *
     * @return Number of confirmed bytes to send back
     */
    public int getAndResetFlowControlData() {
        int ret;

        ret = m_receivedBytes.getAndSet(0);

        // #if LOGGER >= TRACE
        LOGGER.trace("getAndResetFlowControlData (%X): %d", m_destinationNodeID, ret);
        // #endif /* LOGGER >= TRACE */

        return ret;
    }

    /**
     * Called when "confirmed bytes" are received from the remote
     *
     * @param p_confirmedBytes
     *         Number of bytes confirmed by the remote
     */
    public void handleFlowControlData(final int p_confirmedBytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("handleFlowControlData (%X): %d", m_destinationNodeID, p_confirmedBytes);
        // #endif /* LOGGER >= TRACE */

        m_unconfirmedBytes.addAndGet(-p_confirmedBytes);
    }

    @Override
    public String toString() {
        String str;

        str = "FlowControl[m_flowControlWindowSize " + m_flowControlWindowSize + ", m_unconfirmedBytes " + m_unconfirmedBytes + ", m_receivedBytes " +
                m_receivedBytes + ']';

        return str;
    }
}
