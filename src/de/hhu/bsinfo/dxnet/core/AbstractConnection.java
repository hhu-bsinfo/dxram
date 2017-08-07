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
 * Represents a network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.10.2014
 */
public abstract class AbstractConnection<PipeIn extends AbstractPipeIn, PipeOut extends AbstractPipeOut> {

    private final short m_ownNodeID;
    private PipeIn m_pipeIn;
    private PipeOut m_pipeOut;

    private long m_closingTimestamp;

    protected AbstractConnection(final short p_ownNodeId) {
        m_ownNodeID = p_ownNodeId;

        m_closingTimestamp = -1;
    }

    protected short getOwnNodeID() {
        return m_ownNodeID;
    }

    public final short getDestinationNodeID() {
        return m_pipeIn.getDestinationNodeID();
    }

    public final void postMessage(final Message p_message) throws NetworkException {
        m_pipeOut.postMessage(p_message);
    }

    public final PipeIn getPipeIn() {
        return m_pipeIn;
    }

    public final PipeOut getPipeOut() {
        return m_pipeOut;
    }

    public boolean isPipeOutOpen() {
        return m_pipeOut.isOpen();
    }

    public boolean isPipeInOpen() {
        return m_pipeIn.isOpen();
    }

    /**
     * Marks the outgoing connection as (not) connected
     *
     * @param p_connected
     *         if true the connection is marked as connected, otherwise the connections marked as not connected
     */
    public final void setPipeOutConnected(final boolean p_connected) {
        m_pipeOut.setConnected(p_connected);
    }

    /**
     * Marks the  incoming connection as (not) connected
     *
     * @param p_connected
     *         if true the connection is marked as connected, otherwise the connections marked as not connected
     */
    public final void setPipeInConnected(final boolean p_connected) {
        m_pipeIn.setConnected(p_connected);
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
