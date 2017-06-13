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

package de.hhu.bsinfo.net.core;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.10.2014
 */
public abstract class AbstractConnection<PipeIn extends AbstractPipeIn, PipeOut extends AbstractPipeOut> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractConnection.class.getSimpleName());

    private final short m_ownNodeId;
    private PipeIn m_pipeIn;
    private PipeOut m_pipeOut;
    private final MessageCreator m_messageCreator;

    private long m_closingTimestamp;

    protected AbstractConnection(final short p_ownNodeId, final MessageCreator p_messageCreator) {
        m_ownNodeId = p_ownNodeId;
        m_messageCreator = p_messageCreator;

        m_closingTimestamp = -1;
    }

    public short getOwnNodeId() {
        return m_ownNodeId;
    }

    public final short getDestinationNodeId() {
        return m_pipeIn.getDestinationNodeId();
    }

    public final boolean isIncomingQueueFull() {
        return m_messageCreator.isFull();
    }

    public final boolean isCongested() {
        // pipe in and out are expected to have the same flow control objects
        return m_pipeIn.getFlowControl().isCongested() || m_messageCreator.isFull();
    }

    public final void returnProcessedBuffer(final ByteBuffer p_buffer) {
        // #if LOGGER >= TRACE
        LOGGER.trace("Returning processed buffer, size %d", p_buffer.capacity());
        // #endif /* LOGGER >= TRACE */

        m_pipeIn.returnProcessedBuffer(p_buffer);
    }

    public final void postMessage(final AbstractMessage p_message) throws NetworkException {
        m_pipeOut.postMessage(p_message);
    }

    public final PipeIn getPipeIn() {
        return m_pipeIn;
    }

    public final PipeOut getPipeOut() {
        return m_pipeOut;
    }

    public boolean isOutgoingOpen() {
        return m_pipeOut.isOpen();
    }

    public boolean isIncomingOpen() {
        return m_pipeIn.isOpen();
    }

    /**
     * Marks the connection as (not) connected
     *
     * @param p_connected
     *         if true the connection is marked as connected, otherwise the connections marked as not connected
     */
    public final void setConnected(final boolean p_connected, final boolean p_outgoing) {
        if (p_outgoing) {
            m_pipeOut.setConnected(p_connected);
        } else {
            m_pipeIn.setConnected(p_connected);
        }
    }

    /**
     * Get the closing timestamp
     *
     * @return the closing timestamp
     */
    public long getClosingTimestamp() {
        return m_closingTimestamp;
    }

    /**
     * Closes the connection immediately
     */
    public abstract void close(final boolean p_force);

    public abstract void wakeup();

    /**
     * Set the closing timestamp
     */
    protected final void setClosingTimestamp() {
        m_closingTimestamp = System.currentTimeMillis();
    }

    protected final void setPipes(final PipeIn p_pipeIn, final PipeOut p_pipeOut) {
        if (p_pipeIn.getOwnNodeId() != p_pipeOut.getOwnNodeId()) {
            throw new IllegalStateException("Pipe in and pipe out own node ids don't match");
        }

        if (p_pipeIn.getDestinationNodeId() != p_pipeOut.getDestinationNodeId()) {
            throw new IllegalStateException("Pipe in and pipe out destination node ids don't match");
        }

        if (!Objects.equals(p_pipeIn.getFlowControl(), p_pipeOut.getFlowControl())) {
            throw new IllegalStateException("Pipe in and out have different flow controls");
        }

        m_pipeIn = p_pipeIn;
        m_pipeOut = p_pipeOut;
    }

    /**
     * Get the String representation
     *
     * @return the String representation
     */
    @Override
    public String toString() {
        return "Connection: " + m_pipeIn + " | " + m_pipeOut;
    }
}
