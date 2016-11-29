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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a AskAboutBackupsRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutBackupsResponse extends AbstractResponse {

    // Attributes
    private byte[] m_missingMetadata;

    // Constructors

    /**
     * Creates an instance of AskAboutBackupsResponse
     */
    public AskAboutBackupsResponse() {
        super();

        m_missingMetadata = null;
    }

    /**
     * Creates an instance of AskAboutBackupsResponse
     *
     * @param p_request
     *     the corresponding AskAboutBackupsRequest
     * @param p_missingMetadata
     *     the missing metadata
     */
    public AskAboutBackupsResponse(final AskAboutBackupsRequest p_request, final byte[] p_missingMetadata) {
        super(p_request, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE);

        m_missingMetadata = p_missingMetadata;
    }

    // Getters

    /**
     * Get the missing metadata
     *
     * @return the byte array
     */
    public final byte[] getMissingMetadata() {
        return m_missingMetadata;
    }

    @Override
    protected final int getPayloadLength() {
        int ret;

        ret = Integer.BYTES;
        if (m_missingMetadata != null && m_missingMetadata.length > 0) {
            ret += m_missingMetadata.length;
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_missingMetadata == null || m_missingMetadata.length == 0) {
            p_buffer.putInt(0);
        } else {
            p_buffer.putInt(m_missingMetadata.length);
            p_buffer.put(m_missingMetadata);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length;

        length = p_buffer.getInt();
        if (length != 0) {
            m_missingMetadata = new byte[length];
            p_buffer.get(m_missingMetadata);
        }
    }

}
