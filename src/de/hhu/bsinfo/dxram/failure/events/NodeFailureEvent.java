/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.failure.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * An event for node failure
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class NodeFailureEvent extends AbstractEvent {

    private short m_nodeID = NodeID.INVALID_ID;
    private NodeRole m_role = NodeRole.PEER;

    /**
     * Creates an instance of NodeFailureEvent
     *
     * @param p_sourceClass
     *         the calling class
     * @param p_nodeID
     *         the NodeID of the failed peer
     * @param p_role
     *         the failed peer's role
     */
    public NodeFailureEvent(final String p_sourceClass, final short p_nodeID, final NodeRole p_role) {
        super(p_sourceClass);

        m_nodeID = p_nodeID;
        m_role = p_role;
    }

    /**
     * Returns the NodeID
     *
     * @return the failed peer's NodeID
     */
    public short getNodeID() {
        return m_nodeID;
    }

    /**
     * Returns the NodeRole
     *
     * @return the failed peer's role
     */
    public NodeRole getRole() {
        return m_role;
    }

}
