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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Ask About Backups Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutBackupsRequest extends AbstractRequest {

    // Attributes
    private ArrayList<Short> m_peers;
    private int m_numberOfNameserviceEntries;
    private int m_numberOfStorages;
    private int m_numberOfBarriers;

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
     *     the destination
     * @param p_peers
     *     all peers for which this superpeer stores backups
     * @param p_numberOfNameserviceEntries
     *     the number of expected nameservice entries
     * @param p_numberOfStorages
     *     the number of expected storages
     * @param p_numberOfBarriers
     *     the number of expected barriers
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
        int ret;

        ret = Integer.BYTES;
        if (m_peers != null && !m_peers.isEmpty()) {
            ret += Short.BYTES * m_peers.size();
        }
        ret += 3 * Integer.BYTES;

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_peers == null || m_peers.isEmpty()) {
            p_buffer.putInt(0);
        } else {
            p_buffer.putInt(m_peers.size());
            for (short peer : m_peers) {
                p_buffer.putShort(peer);
            }
        }
        p_buffer.putInt(m_numberOfNameserviceEntries);
        p_buffer.putInt(m_numberOfStorages);
        p_buffer.putInt(m_numberOfBarriers);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length;

        m_peers = new ArrayList<Short>();
        length = p_buffer.getInt();
        for (int i = 0; i < length; i++) {
            m_peers.add(p_buffer.getShort());
        }
        m_numberOfNameserviceEntries = p_buffer.getInt();
        m_numberOfStorages = p_buffer.getInt();
        m_numberOfBarriers = p_buffer.getInt();
    }

}
