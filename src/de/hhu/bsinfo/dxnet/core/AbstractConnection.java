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

import java.util.Objects;

/**
 * Represents an abstract network connection implemented by different transports
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.10.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.08.2017
 */
public abstract class AbstractConnection<PipeIn extends AbstractPipeIn, PipeOut extends AbstractPipeOut> {

    private final short m_ownNodeID;
    private PipeIn m_pipeIn;
    private PipeOut m_pipeOut;

    private long m_closingTimestamp;

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current node instance
     */
    protected AbstractConnection(final short p_ownNodeId) {
        m_ownNodeID = p_ownNodeId;

        m_closingTimestamp = -1;
    }

    /**
     * Get the current instance's node id
     */
    protected short getOwnNodeID() {
        return m_ownNodeID;
    }

    /**
     * Get the node id of the destination this connection is connected to
     */
    public final short getDestinationNodeID() {
        return m_pipeIn.getDestinationNodeID();
    }

    /**
     * Post a message to be written to the connection
     *
     * @param p_message
     *         Message to send
     * @throws NetworkException
     *         If posting the message failed (e.g. serializing the message to a buffer)
     */
    public final void postMessage(final Message p_message) throws NetworkException {
        m_pipeOut.postMessage(p_message);
    }

    /**
     * Get the PipeIn endpoint of the connection
     */
    public final PipeIn getPipeIn() {
        return m_pipeIn;
    }

    /**
     * Get the PipeOut endpoint of the connection
     */
    public final PipeOut getPipeOut() {
        return m_pipeOut;
    }

    /**
     * Check if the PipeOut endpoint it open
     */
    public boolean isPipeOutOpen() {
        return m_pipeOut.isOpen();
    }

    /**
     * Check if the PipeIn endpoint is open
     */
    public boolean isPipeInOpen() {
        return m_pipeIn.isOpen();
    }

    /**
     * Marks the outgoing pipe as (not) connected
     */
    public final void setPipeOutConnected(final boolean p_connected) {
        m_pipeOut.setConnected(p_connected);
    }

    /**
     * Marks the incoming pipe as (not) connected
     */
    public final void setPipeInConnected(final boolean p_connected) {
        m_pipeIn.setConnected(p_connected);
    }

    /**
     * Get the timestamp when the connection was closed
     */
    public long getClosingTimestamp() {
        return m_closingTimestamp;
    }

    @Override
    public String toString() {
        return "Connection: " + m_pipeIn + " | " + m_pipeOut;
    }

    /**
     * Closes the connection
     *
     * @param p_force
     *         False to wait until all remaining messages are sent out, true to forcefully close the connection
     */
    public abstract void close(final boolean p_force);

    /**
     * "Wake up" the connection. Depending on the transport type (and how it's) implemented, you have to
     * wake up sleeping threads (refer to available implementations how it's used)
     */
    public abstract void wakeup();

    /**
     * Set the closing timestamp of the connection
     */
    protected final void setClosingTimestamp() {
        m_closingTimestamp = System.currentTimeMillis();
    }

    /**
     * Set the pipes
     *
     * @param p_pipeIn
     *         PipeIn to set
     * @param p_pipeOut
     *         PipeOut to set
     */
    protected final void setPipes(final PipeIn p_pipeIn, final PipeOut p_pipeOut) {
        if (p_pipeIn.getOwnNodeID() != p_pipeOut.getOwnNodeID()) {
            throw new IllegalStateException("Pipe in and pipe out own node ids don't match");
        }

        if (p_pipeIn.getDestinationNodeID() != p_pipeOut.getDestinationNodeID()) {
            throw new IllegalStateException("Pipe in and pipe out destination node ids don't match");
        }

        if (!Objects.equals(p_pipeIn.getFlowControl(), p_pipeOut.getFlowControl())) {
            throw new IllegalStateException("Pipe in and out have different flow controls");
        }

        m_pipeIn = p_pipeIn;
        m_pipeOut = p_pipeOut;
    }
}
