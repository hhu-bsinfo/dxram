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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a AskAboutBackupsRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutBackupsResponse extends Response {

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
     *         the corresponding AskAboutBackupsRequest
     * @param p_missingMetadata
     *         the missing metadata
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
        if (m_missingMetadata != null && m_missingMetadata.length > 0) {
            return ObjectSizeUtil.sizeofByteArray(m_missingMetadata);
        } else {
            return Byte.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_missingMetadata == null || m_missingMetadata.length == 0) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeByteArray(m_missingMetadata);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_missingMetadata = p_importer.readByteArray(m_missingMetadata);
    }

}
