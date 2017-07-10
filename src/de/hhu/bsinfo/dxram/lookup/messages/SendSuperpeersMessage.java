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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;

/**
 * Send Superpeers Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class SendSuperpeersMessage extends AbstractMessage {

    // Attributes
    private ArrayList<Short> m_superpeers;

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
        int ret;

        ret = Integer.BYTES;
        if (m_superpeers != null && !m_superpeers.isEmpty()) {
            ret += Short.BYTES * m_superpeers.size();
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_superpeers == null || m_superpeers.isEmpty()) {
            p_exporter.writeInt(0);
        } else {
            p_exporter.writeInt(m_superpeers.size());
            for (short superpeer : m_superpeers) {
                p_exporter.writeShort(superpeer);
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        int length;

        m_superpeers = new ArrayList<Short>();
        length = p_importer.readInt();
        for (int i = 0; i < length; i++) {
            m_superpeers.add(p_importer.readShort());
        }
    }

}
