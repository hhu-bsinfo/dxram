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

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Ask About Backups Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutBackupsRequest extends Request {

    // Attributes
    private ArrayList<Short> m_peers;
    private int m_numberOfNameserviceEntries;
    private int m_numberOfStorages;
    private int m_numberOfBarriers;

    private int m_length; // Used for serialization, only

    // Constructors

    /**
     * Creates an instance of AskAboutBackupsRequest
     */
    public AskAboutBackupsRequest() {
        super();

        m_peers = null;
    }

    /**
     * Creates an instance of AskAboutBackupsRequest
     *
     * @param p_destination
     *         the destination
     * @param p_peers
     *         all peers for which this superpeer stores backups
     * @param p_numberOfNameserviceEntries
     *         the number of expected nameservice entries
     * @param p_numberOfStorages
     *         the number of expected storages
     * @param p_numberOfBarriers
     *         the number of expected barriers
     */
    public AskAboutBackupsRequest(final short p_destination, final ArrayList<Short> p_peers, final int p_numberOfNameserviceEntries,
            final int p_numberOfStorages, final int p_numberOfBarriers) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST);

        m_peers = p_peers;
        m_numberOfNameserviceEntries = p_numberOfNameserviceEntries;
        m_numberOfStorages = p_numberOfStorages;
        m_numberOfBarriers = p_numberOfBarriers;
    }

    // Getters

    /**
     * Get the peers for which the superpeer stores backups
     *
     * @return the peers
     */
    public final ArrayList<Short> getPeers() {
        return m_peers;
    }

    /**
     * Get the expected number of nameservice entries
     *
     * @return the peers
     */
    public final int getNumberOfNameserviceEntries() {
        return m_numberOfNameserviceEntries;
    }

    /**
     * Get the expected number of storages
     *
     * @return the peers
     */
    public final int getNumberOfStorages() {
        return m_numberOfStorages;
    }

    /**
     * Get the expected number of barriers
     *
     * @return the peers
     */
    public final int getNumberOfBarriers() {
        return m_numberOfBarriers;
    }

    @Override
    protected final int getPayloadLength() {
        int ret = 0;

        if (m_peers != null && !m_peers.isEmpty()) {
            ret = ObjectSizeUtil.sizeofCompactedNumber(m_peers.size()) + Short.BYTES * m_peers.size();
        } else {
            ret = Byte.BYTES;
        }
        ret += 3 * Integer.BYTES;

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_peers == null || m_peers.isEmpty()) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeCompactNumber(m_peers.size());
            for (short peer : m_peers) {
                p_exporter.writeShort(peer);
            }
        }
        p_exporter.writeInt(m_numberOfNameserviceEntries);
        p_exporter.writeInt(m_numberOfStorages);
        p_exporter.writeInt(m_numberOfBarriers);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_length = p_importer.readCompactNumber(m_length);
        if (m_peers == null) {
            m_peers = new ArrayList<Short>(m_length);
        }
        for (int i = 0; i < m_length; i++) {
            short peer = p_importer.readShort((short) 0);
            if (m_peers.size() == i) {
                m_peers.add(peer);
            }
        }
        m_numberOfNameserviceEntries = p_importer.readInt(m_numberOfNameserviceEntries);
        m_numberOfStorages = p_importer.readInt(m_numberOfStorages);
        m_numberOfBarriers = p_importer.readInt(m_numberOfBarriers);
    }

}
