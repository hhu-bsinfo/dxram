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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Send Superpeers Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class SendSuperpeersMessage extends Message {

    // Attributes
    private ArrayList<Short> m_superpeers;

    private int m_superpeersToRead; // For serialization, only

    // Constructors

    /**
     * Creates an instance of SendSuperpeersMessage
     */
    public SendSuperpeersMessage() {
        super();

        m_superpeers = null;
    }

    /**
     * Creates an instance of SendSuperpeersMessage
     *
     * @param p_destination
     *         the destination
     * @param p_superpeers
     *         the superpeers
     */
    public SendSuperpeersMessage(final short p_destination, final ArrayList<Short> p_superpeers) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE);

        assert p_superpeers != null;

        m_superpeers = p_superpeers;
    }

    // Getters

    /**
     * Get the superpeers
     *
     * @return the superpeer array
     */
    public final ArrayList<Short> getSuperpeers() {
        return m_superpeers;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_superpeers == null || m_superpeers.isEmpty()) {
            return Byte.BYTES;
        } else {
            return ObjectSizeUtil.sizeofCompactedNumber(m_superpeers.size()) + Short.BYTES * m_superpeers.size();
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_superpeers == null || m_superpeers.isEmpty()) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeCompactNumber(m_superpeers.size());
            for (short superpeer : m_superpeers) {
                p_exporter.writeShort(superpeer);
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_superpeersToRead = p_importer.readCompactNumber(m_superpeersToRead);
        if (m_superpeers == null) {
            m_superpeers = new ArrayList<Short>(m_superpeersToRead);
        }
        for (int i = 0; i < m_superpeersToRead; i++) {
            short superpeer = p_importer.readShort((short) 0);
            if (m_superpeers.size() == i) {
                m_superpeers.add(superpeer);
            }
        }
    }

}
