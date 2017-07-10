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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractResponse;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Response to a AskAboutSuccessorRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutSuccessorResponse extends AbstractResponse {

    // Attributes
    private short m_successor;

    // Constructors

    /**
     * Creates an instance of AskAboutSuccessorResponse
     */
    public AskAboutSuccessorResponse() {
        super();

        m_successor = NodeID.INVALID_ID;
    }

    /**
     * Creates an instance of AskAboutSuccessorResponse
     *
     * @param p_request
     *         the corresponding AskAboutSuccessorRequest
     * @param p_predecessor
     *         the predecessor
     */
    public AskAboutSuccessorResponse(final AskAboutSuccessorRequest p_request, final short p_predecessor) {
        super(p_request, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE);

        assert p_predecessor != 0;

        m_successor = p_predecessor;
    }

    // Getters

    /**
     * Get the predecessor
     *
     * @return the NodeID
     */
    public final short getSuccessor() {
        return m_successor;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_successor);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_successor = p_importer.readShort();
    }

}
