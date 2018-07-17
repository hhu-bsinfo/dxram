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

/**
 * Send Backups Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class SendBackupsMessage extends Message {

    // Attributes
    private byte[] m_metadata;

    // Constructors

    /**
     * Creates an instance of SendBackupsMessage
     */
    public SendBackupsMessage() {
        super();

        m_metadata = null;
    }

    /**
     * Creates an instance of SendBackupsMessage
     *
     * @param p_destination
     *         the destination
     * @param p_metadata
     *         the metadata
     */
    public SendBackupsMessage(final short p_destination, final byte[] p_metadata) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE);

        m_metadata = p_metadata;
    }

    // Getters

    /**
     * Get metadata
     *
     * @return the byte array
     */
    public final byte[] getMetadata() {
        return m_metadata;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_metadata != null && m_metadata.length > 0) {
            return ObjectSizeUtil.sizeofByteArray(m_metadata);
        } else {
            return Byte.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_metadata == null || m_metadata.length == 0) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeByteArray(m_metadata);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_metadata = p_importer.readByteArray(m_metadata);
    }

}
