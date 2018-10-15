/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Peer Join Event Message. Message to propagate a peer joining to all other peers (two-phase: 1. inform all superpeers 2. superpeers inform peers).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class NodeJoinEventMessage extends Message {

    // Attributes
    private short m_nodeID;
    private NodeRole m_nodeRole;
    private int m_capabilities;
    private short m_rack;
    private short m_switch;
    private boolean m_availableForBackup;
    private IPV4Unit m_address;

    // Temp. state
    private short m_acr;
    private String m_addrStr;

    // Constructors

    /**
     * Creates an instance of NodeJoinEventMessage
     */
    public NodeJoinEventMessage() {
        super();
    }

    /**
     * Creates an instance of NodeJoinEventMessage
     *
     * @param p_destination
     *         the destination
     * @param p_nodeID
     * @param p_capabilities
     */
    public NodeJoinEventMessage(final short p_destination, final short p_nodeID, final NodeRole p_role,
            int p_capabilities, final short p_rack, final short p_switch, final boolean p_availableForBackup,
            final IPV4Unit p_address) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST);

        m_nodeID = p_nodeID;
        m_nodeRole = p_role;
        m_capabilities = p_capabilities;
        m_rack = p_rack;
        m_switch = p_switch;
        m_availableForBackup = p_availableForBackup;
        m_address = p_address;
    }

    // Getters

    /**
     * Get the joined peer
     *
     * @return the NodeID
     */
    public final short getJoinedPeer() {
        return m_nodeID;
    }

    /**
     * Returns the NodeRole
     *
     * @return the joined peer's role
     */
    public NodeRole getRole() {
        return m_nodeRole;
    }

    /**
     * Returns the capabilities.
     *
     * @return The joined peer's capabilities.
     */
    public int getCapabilities() {
        return m_capabilities;
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

    @Override
    protected final int getPayloadLength() {
        return 4 * Short.BYTES + Integer.BYTES + Byte.BYTES + ObjectSizeUtil.sizeofString(m_address.getAddressStr());
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_nodeID);
        p_exporter.writeShort((short) m_nodeRole.getAcronym());
        p_exporter.writeInt(m_capabilities);
        p_exporter.writeShort(m_rack);
        p_exporter.writeShort(m_switch);
        p_exporter.writeBoolean(m_availableForBackup);
        p_exporter.writeString(m_address.getAddressStr());
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_nodeID = p_importer.readShort(m_nodeID);
        m_acr = p_importer.readShort(m_acr);
        m_nodeRole = NodeRole.getRoleByAcronym((char) m_acr);
        m_capabilities = p_importer.readInt(m_capabilities);
        m_rack = p_importer.readShort(m_rack);
        m_switch = p_importer.readShort(m_switch);
        m_availableForBackup = p_importer.readBoolean(m_availableForBackup);
        m_addrStr = p_importer.readString(m_addrStr);
        String[] splitAddr = m_addrStr.split(":");
        m_address = new IPV4Unit(splitAddr[0], Integer.parseInt(splitAddr[1]));
    }

}
