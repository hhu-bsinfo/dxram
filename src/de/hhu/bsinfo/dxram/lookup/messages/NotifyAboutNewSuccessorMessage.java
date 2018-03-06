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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Notify About New Successor Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class NotifyAboutNewSuccessorMessage extends Message {

    // Attributes
    private short m_newSuccessor;

    // Constructors

    /**
     * Creates an instance of NotifyAboutNewSuccessorMessage
     */
    public NotifyAboutNewSuccessorMessage() {
        super();

        m_newSuccessor = NodeID.INVALID_ID;
    }

    /**
     * Creates an instance of NotifyAboutNewSuccessorMessage
     *
     * @param p_destination
     *         the destination
     * @param p_newSuccessor
     *         the new successor
     */
    public NotifyAboutNewSuccessorMessage(final short p_destination, final short p_newSuccessor) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE);

        assert p_newSuccessor != NodeID.INVALID_ID;

        m_newSuccessor = p_newSuccessor;
    }

    // Getters

    /**
     * Get new successor
     *
     * @return the NodeID
     */
    public final short getNewSuccessor() {
        return m_newSuccessor;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_newSuccessor);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_newSuccessor = p_importer.readShort(m_newSuccessor);
    }

}
