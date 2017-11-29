/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.UnsafeHandler;

/**
 * Endpoint for outgoing data on a connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.06.2017
 */
public abstract class AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeOut.class.getSimpleName());

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private boolean m_isConnected;
    private final AbstractFlowControl m_flowControl;
    private final OutgoingRingBuffer m_outgoing;

    private AtomicLong m_sentMessages;
    private AtomicLong m_sentData;

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current instance (sender)
     * @param p_destinationNodeId
     *         Node id of the remote (receiver)
     * @param p_flowControl
     *         FlowControl instance of the connection
     * @param p_outgoingBuffer
     *         OutgoingRingBuffer instance of the connection
     */
    protected AbstractPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final OutgoingRingBuffer p_outgoingBuffer) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;

        m_flowControl = p_flowControl;
        m_outgoing = p_outgoingBuffer;

        m_sentMessages = new AtomicLong(0);
        m_sentData = new AtomicLong(0);
    }

    /**
     * Get the node id of the destination receiving sent data
     */
    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    /**
     * Check if the pipe is connected to the remote
     */
    public boolean isConnected() {
        return m_isConnected;
    }

    /**
     * Set the pipe connected to the remote
     */
    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
        UnsafeHandler.getInstance().getUnsafe()
                .storeFence(); // m_isConnected is not volatile as it is read often without changing value -> use store fence here
    }

    /**
     * Get the FlowControl instance connected to the pipe
     */
    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    /**
     * Check if the outgoing buffer is currently empty
     *
     * @return True if empty, false otherwise
     */
    public boolean isOutgoingQueueEmpty() {
        return m_outgoing.isEmpty();
    }

    /**
     * Call this when your transport finished sending out data
     *
     * @param p_writtenBytes
     *         Number of bytes recently sent
     */
    public void dataProcessed(final int p_writtenBytes) {
        m_outgoing.shiftBack(p_writtenBytes);
    }

    /**
     * Get the node id of the current instance
     */
    short getOwnNodeID() {
        return m_ownNodeID;
    }

    /**
     * Post a message to this pipe
     *
     * @param p_message
     *         Message to post
     * @throws NetworkException
     *         If deserializing the message to the buffer or posting a write request failed
     */
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
        m_flowControl.dataToSend(messageTotalSize);

        m_sentMessages.incrementAndGet();
        m_sentData.addAndGet(messageTotalSize);

        // at this point, we consider the message "sent"
        p_message.setSendReceiveTimestamp(System.nanoTime());

        m_outgoing.pushMessage(p_message, messageTotalSize, this);
    }

    /**
     * Check if the pipe is opened
     */
    protected abstract boolean isOpen();

    /**
     * Callback when a message is deserialized to the outgoing buffer. Trigger your backend
     * to send the data.
     *
     * @param p_size
     *         Number of bytes posted and ready to send
     */
    protected abstract void bufferPosted(final int p_size);

    /**
     * Get the OutgoingRingBuffer of this pipe/connection
     */
    protected OutgoingRingBuffer getOutgoingQueue() {
        return m_outgoing;
    }
}
