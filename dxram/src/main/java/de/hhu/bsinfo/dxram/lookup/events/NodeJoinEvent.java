/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * An event for node joining
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.03.2017
 */
public class NodeJoinEvent extends AbstractEvent {

    private short m_nodeID = NodeID.INVALID_ID;
    private NodeRole m_role = NodeRole.PEER;
    private short m_rack = 0;
    private short m_switch = 0;
    private boolean m_availableForBackup;
    private IPV4Unit m_address;

    /**
     * Creates an instance of NodeJoinEvent
     *
     * @param p_sourceClass
     *         the calling class
     * @param p_nodeID
     *         the NodeID of the failed peer
     * @param p_role
     *         the joined peer's role
     */
    public NodeJoinEvent(final String p_sourceClass, final short p_nodeID, final NodeRole p_role, final short p_rack, final short p_switch,
            final boolean p_availableForBackup, final IPV4Unit p_address) {
        super(p_sourceClass);

        m_nodeID = p_nodeID;
        m_role = p_role;
        m_rack = p_rack;
        m_switch = p_switch;
        m_availableForBackup = p_availableForBackup;
        m_address = p_address;
    }

    /**
     * Returns the NodeID
     *
     * @return the joined peer's NodeID
     */
    public short getNodeID() {
        return m_nodeID;
    }

    /**
     * Returns the NodeRole
     *
     * @return the joined peer's role
     */
    public NodeRole getRole() {
        return m_role;
    }

    /**
     * Returns the rack
     *
     * @return the joined peer's rack
     */
    public short getRack() {
        return m_rack;
    }

    /**
     * Returns the switch
     *
     * @return the joined peer's switch
     */
    public short getSwitch() {
        return m_switch;
    }

    /**
     * Returns whether the joined node is available for backup or not. Always false for superpeers.
     *
     * @return true, if available for backup
     */
    public boolean isAvailableForBackup() {
        return m_availableForBackup;
    }

    /**
     * Returns the address
     *
     * @return the joined peer's address
     */
    public IPV4Unit getAddress() {
        return m_address;
    }
}
