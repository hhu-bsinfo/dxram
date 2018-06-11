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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Message to inform all nodes about finished startup.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 24.05.2018
 */
public class FinishedStartupMessage extends Message {

    // Constructors
    private short m_rack;
    private short m_switch;
    private boolean m_availableForBackup;
    private int m_capabilities;
    private IPV4Unit m_address;

    // Temp. state
    private String m_addrStr;

    /**
     * Creates an instance of FinishedStartupMessage
     */
    public FinishedStartupMessage() {
        super();
    }

    /**
     * Creates an instance of FinishedStartupMessage
     *
     * @param p_destination
     *         the destination
     * @param p_capabilities
     */
    public FinishedStartupMessage(final short p_destination, final short p_rack, final short p_switch, final boolean p_availableForBackup,
                                  int p_capabilities, final IPV4Unit p_address) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_FINISHED_STARTUP_MESSAGE);
        m_rack = p_rack;
        m_switch = p_switch;
        m_availableForBackup = p_availableForBackup;
        m_capabilities = p_capabilities;
        m_address = p_address;
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
     * Returns whether the joined peer is available for backup or not
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

    /**
     * Returns the capabilities.
     *
     * @return The joined peer's capabilities.
     */
    public int getCapabilities() {
        return m_capabilities;
    }

    @Override
    protected final int getPayloadLength() {
        return 2 * Short.BYTES + Byte.BYTES + Integer.BYTES + ObjectSizeUtil.sizeofString(m_address.getAddressStr());
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rack);
        p_exporter.writeShort(m_switch);
        p_exporter.writeBoolean(m_availableForBackup);
        p_exporter.writeInt(m_capabilities);
        p_exporter.writeString(m_address.getAddressStr());
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_rack = p_importer.readShort(m_rack);
        m_switch = p_importer.readShort(m_switch);
        m_availableForBackup = p_importer.readBoolean(m_availableForBackup);
        m_capabilities = p_importer.readInt(m_capabilities);
        m_addrStr = p_importer.readString(m_addrStr);
        String[] splitAddr = m_addrStr.split(":");
        m_address = new IPV4Unit(splitAddr[0], Integer.parseInt(splitAddr[1]));
    }

}
