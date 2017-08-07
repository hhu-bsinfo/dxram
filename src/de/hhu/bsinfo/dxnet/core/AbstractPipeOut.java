package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * Created by nothaas on 6/9/17.
 */
public abstract class AbstractPipeOut {
    private static final StatisticsOperation SOP_FC_DATA_TO_SEND = StatisticsRecorderManager.getOperation(AbstractPipeOut.class, "FCDataToSend");

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeOut.class.getSimpleName());

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private volatile boolean m_isConnected;
    private final AbstractFlowControl m_flowControl;
    private final OutgoingRingBuffer m_outgoing;

    private AtomicLong m_sentMessages;
    private AtomicLong m_sentData;

    protected AbstractPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final OutgoingRingBuffer p_outgoingBuffer) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;

        m_flowControl = p_flowControl;
        m_outgoing = p_outgoingBuffer;

        m_sentMessages = new AtomicLong(0);
        m_sentData = new AtomicLong(0);
    }

    short getOwnNodeID() {
        return m_ownNodeID;
    }

    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    public boolean isConnected() {
        return m_isConnected;
    }

    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
    }

    public long getSentMessageCount() {
        return m_sentMessages.get();
    }

    public long getSentDataBytes() {
        return m_sentData.get();
    }

    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    public boolean isOutgoingQueueEmpty() {
        return m_outgoing.isEmpty();
    }

    void postMessage(final Message p_message) throws NetworkException {
        // #if LOGGER >= TRACE
        LOGGER.trace("Writing message %s to pipe out of dest 0x%X", p_message, m_destinationNodeID);
        // #endif /* LOGGER >= TRACE */

        int messageTotalSize = p_message.getTotalSize();

        if (messageTotalSize > 1024 * 1024 * 128) {
            // #if LOGGER >= WARN
            LOGGER.warn("Performance warning: Sending very large (%d bytes) message. Consider splitting your data to send if possible to benefit from " +
                    "parallelism when messages are received and processed", messageTotalSize);
            // #endif /* LOGGER >= WARN */
        }

        // #ifdef STATISTICS
        SOP_FC_DATA_TO_SEND.enter();
        // #endif /* STATISTICS */

        m_flowControl.dataToSend(messageTotalSize);

        // #ifdef STATISTICS
        SOP_FC_DATA_TO_SEND.leave();
        // #endif /* STATISTICS */

        m_sentMessages.incrementAndGet();
        m_sentData.addAndGet(messageTotalSize);

        m_outgoing.pushMessage(p_message, messageTotalSize, this);
    }

    public void dataProcessed(final int p_writtenBytes) {
        m_outgoing.shiftFront(p_writtenBytes);
    }

    protected abstract boolean isOpen();

    protected abstract void bufferPosted(final int p_size);

    protected OutgoingRingBuffer getOutgoingQueue() {
        return m_outgoing;
    }
}
