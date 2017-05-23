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

package de.hhu.bsinfo.dxram.net.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.utils.NodeID;

/**
 * An event for connection loss. Triggered by NIOSelector-Thread.
 * Caused by a problem with the connection to given Node.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2016
 */
public class ConnectionLostEvent extends AbstractEvent {

    private short m_nodeID = NodeID.INVALID_ID;

    /**
     * Creates an instance of NodeFailureEvent
     *
     * @param p_sourceClass
     *     the calling class
     * @param p_nodeID
     *     the NodeID of the failed peer
     */
    public ConnectionLostEvent(final String p_sourceClass, final short p_nodeID) {
        super(p_sourceClass);

        m_nodeID = p_nodeID;
    }

    /**
     * Returns the NodeID
     *
     * @return the failed peer's NodeID
     */
    public short getNodeID() {
        return m_nodeID;
    }
}
